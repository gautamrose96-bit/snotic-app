"""Live activity feed — agent abhi kya kar raha hai, desktop ki screen tak.

════════════════════════════════════════════════════════════════════════════
 YE FILE KYUN BANI
════════════════════════════════════════════════════════════════════════════

Stonic ki chat window Hermes ke DASHBOARD se juri hui hai (`/api/ws`). Magar
har kaam wahan nahi hota:

    phone se aayi command      ─┐
    Agent Town ke teammates    ─┼─▶  Hermes ka GATEWAY process
    cron ka waqt par chalne    ─┘

Gateway ek alag process hai, aur uske live events dashboard tak jate hi nahi.
Nateeja user ki nazar se bilkul saaf tha: chat kholo aur wo "atki hui" lagti
hai — andar teen tool chal chuke hote hain aur screen par patta tak nahi
hilta.

Ye file wohi khala bharti hai. Hermes ke apne plugin hooks se har tool ka
shuru aur anjaam pakar kar desktop ke local bridge par bhej deti hai, aur
wahan se wo seedha chat window mein tool cards ban jate hain.

════════════════════════════════════════════════════════════════════════════
 HERMES KO HAATH NAHI LAGAYA GAYA
════════════════════════════════════════════════════════════════════════════

Ye Hermes ka apna, dastavez-shuda raasta hai — `pre_tool_call` aur
`post_tool_call` (hermes_cli/plugins.py: VALID_HOOKS). Hermes har callback ko
apne try/except mein bulata hai, aur observer callback ka `None` lautana uske
liye bilkul aam baat hai. Yani yahan ki koi bhi kharabi agent ke turn tak
pohnch hi nahi sakti.

Hook mein `session_id` bhi aata hai — yehi wo dhaaga hai jis se desktop
pehchanta hai ke ye kaam KIS chat ka hai.

════════════════════════════════════════════════════════════════════════════
 TEEN USOOL JIN PAR YE POORI FILE KHARI HAI
════════════════════════════════════════════════════════════════════════════

 1. TURN KABHI NA RUKE. Hook agent ke apne loop ke andar, usi thread par
    chalta hai. Network wahan se chhoona ek sanjeeda ghalti hoti: bridge ka
    ek sust jawab seedha user ke intezaar mein jur jata. Is liye hook sirf
    ek qatar mein daal kar foran wapas aa jata hai, aur bhejne ka kaam ek
    alag, background thread karti hai.

 2. QATAR BHAR JAYE TO NAYA GIRA DO. Ye sirf DIKHANE ki cheez hai. Desktop
    band ho ya bridge jawab na de, to yahan cheezein jama hoti rahein — wo
    memory ka rasta hai. Ek chhoti hadd, aur us se aage khamoshi se gir jana
    hi theek bartao hai.

 3. YE SAB SIRF ISI COMPUTER PAR REHTA HAI. Tool ke args aur natije mein
    file ke raaste, API ke jawab, aur hassas cheezein hoti hain. Ye 127.0.0.1
    par desktop app tak jata hai aur wahin screen par dikhta hai — bilkul
    waise hi jaise desktop ki apni chat window mein dikhta hai. Cloud par,
    ya phone tak, ye KABHI nahi jata (dekho src/lib/remote/link.ts, jahan
    `activity` frame jaan bujh kar phone wale raaste se bahar rakha gaya hai).
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Bridge par is naam ka frame jata hai. Desktop isi se pehchanta hai ke ye
# phone ke liye nahi, apni screen ke liye hai.
FRAME_KIND = "activity"

# ── Haddein ─────────────────────────────────────────────────────────────────
#
# Tool ke args aur natije be-hisaab bare ho sakte hain (ek poori file parh li
# gayi ho, ya ek bara API jawab). Chat window ko un ka sirf ek jhalak chahiye —
# poora matn wahan waise bhi nahi samata. Kaat kar bhejna do faide deta hai:
# bridge par bojh nahi parta, aur ek bhaari tool ka natija app ki memory nahi
# kha jata.
MAX_ARGS_CHARS = 1_000
MAX_RESULT_CHARS = 2_000

# Qatar ki hadd. Ek aam turn mein 5-20 tool events hote hain; 256 us se kahin
# zyada hai. Is se aage ka matlab hai ke doosri taraf koi sun hi nahi raha.
MAX_QUEUE = 256

# Bridge ko jawab dene ke liye itna waqt. Chhota jaan bujh kar hai: ye kaam
# background thread ka hai, magar ek marta hua desktop us thread ko bhi
# hamesha ke liye nahi rok sakta.
POST_TIMEOUT_S = 5.0

# Bridge na mile to itni der khamosh rehna — har event par dobara koshish
# karna sirf logs bharta hai aur CPU khata hai.
BACKOFF_S = 15.0


def _bridge() -> Optional[tuple]:
    """Bridge ka pata aur uski chaabi — dono mojood hon tabhi.

    Desktop app ye dono env vars khud likhti hai (`PUT /api/env`). Gateway
    unhein apne start par parhta hai, is liye yahan har dafa parhna sasta bhi
    hai aur sahi bhi — app ne beech mein port badla ho to agla event nayi
    jagah chala jata hai.
    """
    url = (os.getenv("STONIC_BRIDGE_URL", "") or "").strip().rstrip("/")
    key = (os.getenv("STONIC_BRIDGE_KEY", "") or "").strip()
    if not url or not key:
        return None
    return url, key


def _clip(value: Any, limit: int) -> str:
    """Kisi bhi cheez ko ek mehdood, parhne ke qabil string mein badalna."""
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


class _Publisher:
    """Ek background thread jo qatar khali karti rehti hai.

    Thread pehle event par banti hai (`_ensure_thread`), is liye jin
    installs par ye feature chalta hi nahi wahan ek bhi zaid thread nahi
    banti. Daemon is liye ke gateway band hone par ye kisi ko rok na sake.
    """

    def __init__(self) -> None:
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=MAX_QUEUE)
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._quiet_until = 0.0
        self._dropped = 0

    def publish(self, frame: Dict[str, Any]) -> None:
        """Qatar mein daalo aur FORAN wapas. Yahan se kuch bhi nahi rukta."""
        if _bridge() is None:
            return
        try:
            self._queue.put_nowait(frame)
        except queue.Full:
            # Doosri taraf koi sun nahi raha. Ginti rakh lete hain taake logs
            # mein ek dafa saaf nazar aaye ke kya ho raha tha.
            self._dropped += 1
            if self._dropped % 100 == 1:
                logger.debug("stonic activity: qatar bhari, %d events gire", self._dropped)
            return
        self._ensure_thread()

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._drain,
                name="stonic-activity",
                daemon=True,
            )
            self._thread.start()

    def _drain(self) -> None:
        while True:
            try:
                frame = self._queue.get(timeout=30.0)
            except queue.Empty:
                # Kaam khatam. Thread band ho jati hai; agla event usay dobara
                # bana lega.
                with self._lock:
                    if self._queue.empty():
                        self._thread = None
                        return
                continue
            self._send(frame)

    def _send(self, frame: Dict[str, Any]) -> None:
        if time.monotonic() < self._quiet_until:
            return
        target = _bridge()
        if target is None:
            return
        url, key = target
        try:
            request = urllib.request.Request(
                f"{url}/v1/out",
                data=json.dumps(frame, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=POST_TIMEOUT_S):
                pass
        except Exception as err:  # noqa: BLE001 - har kharabi barabar hai
            # Desktop band hai ya restart ho rahi hai — ye bilkul aam haalat
            # hai, koi kharabi nahi. Thori der khamosh ho jate hain.
            self._quiet_until = time.monotonic() + BACKOFF_S
            logger.debug("stonic activity: bridge tak nahi pohncha (%s)", err)


_publisher = _Publisher()


# ── Hermes ke hooks ─────────────────────────────────────────────────────────
#
# Dono callbacks `**kwargs` lete hain aur `None` lautate hain. Ye do baatein
# ittefaqi nahi hain:
#
#   • Hermes waqt ke saath naye kwargs bhejta rehta hai (misal
#     `telemetry_schema_version`). Sakht signature likhne ka matlab hota ke
#     agle Hermes update par ye chup-chaap kaam karna band kar de.
#
#   • `pre_tool_call` ka lauta hua dict ek POLICY faisla samjha jata hai —
#     `{"action": "block"}` tool ko rok deta hai. Hum sirf dekhne wale hain,
#     rokne wale nahi, is liye yahan se hamesha `None`.


def on_pre_tool_call(**kwargs: Any) -> None:
    """Ek tool shuru hua."""
    try:
        session_id = str(kwargs.get("session_id") or "").strip()
        if not session_id:
            # Bina session ke desktop ye nahi jaan sakta ke kaun si chat hai —
            # aur ghalat chat mein dikhana na dikhane se bura hai.
            return
        _publisher.publish({
            "kind": FRAME_KIND,
            "event": "tool.start",
            "session_id": session_id,
            "tool_id": str(kwargs.get("tool_call_id") or "") or None,
            "name": str(kwargs.get("tool_name") or "tool"),
            "args": _clip(kwargs.get("args"), MAX_ARGS_CHARS),
            "ts": time.time(),
        })
    except Exception as err:  # noqa: BLE001
        logger.debug("stonic activity: pre_tool_call frame nahi ban saka (%s)", err)
    return None


def on_post_tool_call(**kwargs: Any) -> None:
    """Ek tool khatam hua — natije ke saath."""
    try:
        session_id = str(kwargs.get("session_id") or "").strip()
        if not session_id:
            return
        status = str(kwargs.get("status") or "").strip().lower()
        error = kwargs.get("error_message")
        _publisher.publish({
            "kind": FRAME_KIND,
            "event": "tool.complete",
            "session_id": session_id,
            "tool_id": str(kwargs.get("tool_call_id") or "") or None,
            "name": str(kwargs.get("tool_name") or "tool"),
            "result": _clip(kwargs.get("result"), MAX_RESULT_CHARS),
            "error": _clip(error, MAX_ARGS_CHARS) or None,
            # Hermes "ok" / "error" bhejta hai. Khali ho to error ki mojoodgi
            # se faisla — yani shak ka faida kamyabi ko nahi, sach ko.
            "ok": (status == "ok") if status else not error,
            "duration_ms": kwargs.get("duration_ms"),
            "ts": time.time(),
        })
    except Exception as err:  # noqa: BLE001
        logger.debug("stonic activity: post_tool_call frame nahi ban saka (%s)", err)
    return None


def register_hooks(ctx) -> None:
    """Dono hooks Hermes ke plugin system par lagana."""
    ctx.register_hook("pre_tool_call", on_pre_tool_call)
    ctx.register_hook("post_tool_call", on_post_tool_call)
