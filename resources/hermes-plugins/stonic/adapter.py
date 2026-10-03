"""Stonic Mobile platform adapter (Hermes plugin).

Stonic ki mobile app (app.stonicai.com) ko Hermes ka ek poora messaging
platform bana deta hai — WhatsApp, Telegram aur Discord ke barabar ka
darja, un hi ke saath ek hi qatar mein.

════════════════════════════════════════════════════════════════════════════
 YE PLUGIN KYUN, KOI APNA RAASTA KYUN NAHI
════════════════════════════════════════════════════════════════════════════

Pehle Stonic desktop app khud Hermes ke chat gateway se juri thi aur khud
hi jawab jama kar ke phone tak bhejti thi. Wo chalta tha, magar wo Hermes
ki naqal thi — aur naqal hamesha asal se peeche rehti hai:

  - tool ka progress ("🔍 web_search: …")  — haath se banana parta
  - sochne ka amal (thinking relay)         — bilkul nahi tha
  - streaming jawab                         — sirf aakhir mein aata tha
  - /model, /stop, /new aur baqi commands   — kaam hi nahi karte the
  - clarify aur approval ke buttons         — nahi thay
  - cron se khud paigham bhejna             — mumkin hi nahi tha
  - send_message tool se phone par bhejna   — mumkin hi nahi tha

Ye sab Hermes ke gateway (gateway/run.py) mein pehle se mojood hai. Ek
platform adapter ban jane se ye sab MUFT mil jata hai, aur kal Hermes mein
koi naya feature aaya to wo bhi khud-ba-khud phone tak pohnch jayega —
kyunki hum Hermes ke apne raaste par hain, uske samanantar apna raasta bana
kar nahi chal rahe.

════════════════════════════════════════════════════════════════════════════
 YE ADAPTER INTERNET SE BAAT NAHI KARTA
════════════════════════════════════════════════════════════════════════════

Phone tak paigham Supabase Realtime se jata hai, magar wo kaam YE adapter
nahi karta — Stonic desktop app (Electron) karti hai. Ye adapter sirf usi
app ke andar chalte hue ek chhote se local bridge se juda rehta hai:

    Hermes plugin  ──NDJSON stream──▶  127.0.0.1 bridge  ──▶  Supabase  ──▶  📱
                   ◀──── HTTP POST ───

Ye tarteeb jaan bujh kar hai. Hermes ke Python packages exact-pinned hain
aur unka apna likha hua usool hai: "sirf wo package jo HAR session istemal
kare". Supabase ka Python client us shart par poora nahi utarta, aur us
runtime ko chherna hamare pehle usool ("Hermes ko haath nahi lagana") ke
khilaf hai.

Hermes ne khud yehi raasta dikhaya hai: WhatsApp ka adapter bhi Python mein
hai magar asal WhatsApp se ek local Node bridge baat karta hai. Hum wahi
kar rahe hain — bas bridge hamari apni desktop app hai, jis mein Supabase
ka poora kaam pehle se likha hua hai.

Nateeja: is plugin ko sirf ``httpx`` chahiye, jo Hermes ki core dependency
hai. Koi nayi cheez install nahi hoti.

════════════════════════════════════════════════════════════════════════════
 CONFIG
════════════════════════════════════════════════════════════════════════════

Ye sab env vars Stonic desktop app KHUD likhti hai (Hermes ke apne
``PUT /api/env`` se) — user ko kuch bharna nahi parta::

    STONIC_BRIDGE_URL      http://127.0.0.1:<port>   (lazmi)
    STONIC_BRIDGE_KEY      shared secret             (lazmi)
    STONIC_CHAT_ID         Stonic account ka user id
    STONIC_ALLOWED_USERS   wohi user id
    STONIC_HOME_CHANNEL    wohi user id (cron ke liye)

════════════════════════════════════════════════════════════════════════════
 PEHCHAN AUR BHAROSA
════════════════════════════════════════════════════════════════════════════

Bridge sirf 127.0.0.1 par sunta hai aur har request par ek secret maangta
hai jo sirf isi machine par mojood hai. Us se pehle ka poora pehra Supabase
par hai: phone ko us account ka signed token dikhana parta hai jis ka
channel hai, warna paigham yahan tak pohnchta hi nahi.

``user_id`` bridge deta hai (Supabase ka tasdeeq-shuda user id) — paighaam
ke andar likhi hui kisi cheez se nahi banta. Ye farq ahem hai: paighaam ka
matn bhejne wale ke ikhtiyar mein hota hai, us par pehchan nahi rakhi ja
sakti.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - httpx is a core Hermes dependency
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "stonic"

# Phone ki chat par lamba jawab bura nahi lagta (WhatsApp ki tarah 4096 ki
# koi sakht hadd nahi), magar be-hadd bhi nahi chhorna: Supabase Realtime ka
# ek broadcast 256 KB tak jata hai, aur us se bara paigham chup-chaap gir
# jata. 12,000 harf us hadd se bohat neeche hain aur phone par aaram se parhe
# jate hain. Is se lamba jawab kai paighaam mein bat jata hai.
MAX_MESSAGE_LENGTH = 12_000

# Bridge girne par dobara jorne ke waqfe. Bridge wahi desktop app hai jo is
# Hermes ko chala rahi hai — wo band ho to Hermes bhi der tak nahi rehta,
# is liye lambi backoff ki zaroorat nahi.
RECONNECT_BACKOFF = [1, 2, 3, 5, 10]

# Bridge har 20 second par ek ping bhejta hai. 60 second tak kuch na aaye to
# connection mara hua samajh lo aur nayi koshish karo.
STREAM_TIMEOUT_SECONDS = 60

DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 500


class _FatalStreamError(Exception):
    """Aisi kharabi jis ke baad dobara koshish be-faida hai (misal: 401)."""


def _bridge_url() -> str:
    return (os.getenv("STONIC_BRIDGE_URL", "") or "").strip().rstrip("/")


def _bridge_key() -> str:
    return (os.getenv("STONIC_BRIDGE_KEY", "") or "").strip()


def _auth_headers(key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {key}"} if key else {}


def _default_chat_id() -> str:
    return (os.getenv("STONIC_CHAT_ID", "") or "").strip()


class StonicAdapter(BasePlatformAdapter):
    """Stonic mobile adapter — local bridge ke zariye phone tak.

    Inbound  : bridge se NDJSON stream (har line ek paighaam)
    Outbound : bridge par HTTP POST
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    # Phone ki chat markdown render karti hai (code blocks samet) — desktop
    # app ka apna chat bhi wahi karta hai. Is liye Hermes ka jawab jaisa hai
    # waisa hi bhejte hain; tool-progress bhi asli fenced code block mein.
    supports_code_blocks = True

    # Ek turn khatam hone ke BAAD bhi hum paighaam bhej sakte hain — bridge
    # aur Supabase ka channel khula rehta hai. Is ke baghair Hermes background
    # kaam ke mukammal hone ki khabar dene se inkaar kar deta hai (aur wo bhi
    # theek hi karta hai — aisa waada nahi karna chahiye jo poora na ho sake).
    supports_async_delivery = True

    # `send()` khud lambe jawab ko tukron mein bant-ta hai, is liye gateway ko
    # apni taraf se kaatne ki zaroorat nahi.
    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))

        extra = config.extra or {}
        self._url: str = (extra.get("bridge_url") or _bridge_url()).rstrip("/")
        self._key: str = extra.get("bridge_key") or _bridge_key()
        self._chat_id: str = str(extra.get("chat_id") or _default_chat_id() or "stonic")

        self._stream_task: Optional[asyncio.Task] = None
        self._http: Optional["httpx.AsyncClient"] = None
        self._seen: Dict[str, float] = {}

        # Kis chat par abhi ek turn chal raha hai. `_keep_typing` ise bharta
        # hai; bridge ise phone tak pohncha deta hai taake wahan "kaam ho raha
        # hai" dikh sake aur voice assistant jaan sake ke jawab aa chuka.
        self._turns: set = set()

        # Wo tasks jo cancel ke daayre se bahar bheji gayin (dekhein
        # `_keep_typing`). Reference rakhna LAZMI hai: asyncio sirf kamzor
        # reference rakhta hai, aur bina is ke Python task ko beech mein hi
        # utha sakta hai — paighaam kabhi bhejta hi nahi.
        self._detached: set = set()

    # ── Connection ──────────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not HTTPX_AVAILABLE:
            logger.warning("[%s] httpx nahi mila.", self.name)
            return False
        if not self._url or not self._key:
            logger.warning(
                "[%s] STONIC_BRIDGE_URL ya STONIC_BRIDGE_KEY set nahi. "
                "Ye Stonic desktop app khud likhti hai — kya wo chal rahi hai?",
                self.name,
            )
            return False

        try:
            self._http = httpx.AsyncClient(timeout=None)
            self._stream_task = asyncio.create_task(self._run_stream())
            self._mark_connected()
            logger.info("[%s] Bridge se juda: %s", self.name, self._url)
            return True
        except Exception as e:
            logger.error("[%s] Connect fail hua: %s", self.name, e)
            return False

    async def disconnect(self) -> None:
        self._running = False
        self._mark_disconnected()

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        if self._http:
            await self._http.aclose()
            self._http = None

        self._seen.clear()
        self._turns.clear()
        logger.info("[%s] Bridge se alag ho gaya", self.name)

    async def _run_stream(self) -> None:
        """Bridge ka stream — girte hi dobara jur jata hai."""
        backoff = 0
        while self._running:
            started = time.monotonic()
            try:
                await self._consume_stream()
            except asyncio.CancelledError:
                return
            except _FatalStreamError:
                self._running = False
                return
            except Exception as e:
                if not self._running:
                    return
                logger.debug("[%s] Stream toota: %s", self.name, e)

            if not self._running:
                return

            # Ek minute se zyada chala to backoff bhool jao — ye ek nayi
            # kharabi hai, purani ka silsila nahi.
            if time.monotonic() - started >= 60.0:
                backoff = 0
            delay = RECONNECT_BACKOFF[min(backoff, len(RECONNECT_BACKOFF) - 1)]
            await asyncio.sleep(delay)
            backoff += 1

    async def _consume_stream(self) -> None:
        """Bridge ka NDJSON stream parhna — har line ek paighaam."""
        assert self._http is not None
        url = f"{self._url}/v1/in"

        async with self._http.stream(
            "GET",
            url,
            headers=_auth_headers(self._key),
            timeout=httpx.Timeout(
                connect=10.0, read=STREAM_TIMEOUT_SECONDS, write=10.0, pool=10.0
            ),
        ) as response:
            if response.status_code in (401, 403):
                logger.error(
                    "[%s] Bridge ne key qubool nahi ki (%d). Stonic app dobara "
                    "khol kar dekhein.", self.name, response.status_code,
                )
                self._set_fatal_error(
                    "stonic_unauthorized",
                    "Stonic bridge ne key rad kar di. Desktop app dobara chalayein.",
                    retryable=False,
                )
                raise _FatalStreamError("unauthorized")
            response.raise_for_status()

            # Stream khul gaya — bridge ab jaanta hai ke Hermes tayyar hai,
            # aur wo phone par sabz nuqta jala deta hai.
            logger.info("[%s] Bridge ka stream khul gaya", self.name)

            async for line in response.aiter_lines():
                if not self._running:
                    return
                line = line.strip()
                if not line:
                    continue
                try:
                    frame = json.loads(line)
                except json.JSONDecodeError:
                    continue

                kind = frame.get("type")
                if kind == "ping":
                    continue
                if kind == "message":
                    await self._on_message(frame)

    # ── Inbound ─────────────────────────────────────────────────────────────

    async def _on_message(self, frame: Dict[str, Any]) -> None:
        message_id = str(frame.get("id") or uuid.uuid4().hex)
        if self._is_duplicate(message_id):
            return

        text = str(frame.get("text") or "").strip()
        if not text:
            return

        # ── PEHCHAN BRIDGE SE AATI HAI, PAIGHAAM SE NAHI ────────────────────
        #
        # `user_id` wo Supabase user id hai jise Supabase ne khud verify kiya
        # aur desktop app ne bridge tak pohnchaya. Paighaam ke andar likhi hui
        # kisi cheez par pehchan nahi rakhi jati — matn bhejne wale ke
        # ikhtiyar mein hota hai, aur us par bharosa karna authorization ko
        # be-maani kar deta.
        user_id = str(frame.get("user_id") or self._chat_id)
        chat_id = str(frame.get("chat_id") or user_id)
        user_name = str(frame.get("user_name") or "Stonic")

        source = self.build_source(
            chat_id=chat_id,
            chat_name="Stonic Mobile",
            chat_type="dm",
            user_id=user_id,
            user_name=user_name,
            message_id=message_id,
        )

        ts = frame.get("ts")
        try:
            timestamp = (
                datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc)
                if ts else datetime.now(tz=timezone.utc)
            )
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
            raw_message=frame,
            timestamp=timestamp,
        )

        logger.debug("[%s] Phone se: %s", self.name, text[:80])
        await self.handle_message(event)

    def _is_duplicate(self, message_id: str) -> bool:
        now = time.time()
        if len(self._seen) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        if message_id in self._seen:
            return True
        self._seen[message_id] = now
        return False

    # ── Outbound ────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._http:
            return SendResult(success=False, error="Stonic bridge se rabta nahi hai")

        chunks = self.truncate_message(content, max_length=self.MAX_MESSAGE_LENGTH)
        last_id: Optional[str] = None

        for chunk in chunks:
            payload: Dict[str, Any] = {
                "kind": "message",
                "chat_id": str(chat_id or self._chat_id),
                "text": chunk,
                "message_id": uuid.uuid4().hex,
            }
            if reply_to:
                payload["reply_to"] = str(reply_to)

            result = await self._post(payload)
            if not result.get("ok"):
                return SendResult(success=False, error=str(result.get("error") or "bridge send fail"))
            last_id = payload["message_id"]

        return SendResult(success=True, message_id=last_id or uuid.uuid4().hex)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Ek bheja hua paighaam badal dena.

        ── YE METHOD KYUN HAI (SIRF "achha lagta hai" NAHI) ────────────────

        Hermes tool ka progress ek NAYE paighaam ke tor par nahi bhejta — wo
        ek hi bubble banata hai aur usay baar baar badalta rehta hai:

            🔍 web_search: "gpt-5.6 pricing"
            🔍 web_search: "gpt-5.6 pricing"
            📄 read_file: report.md          ← wohi bubble, nayi lakeer

        Aur uska faisla bilkul saaf hai (gateway/run.py, send_progress_messages):

            if type(adapter).edit_message is BasePlatformAdapter.edit_message:
                ...saara progress chup-chaap phenk do...

        Yani jo adapter ye method NAHI likhta, Hermes uske liye tool progress
        bhejta hi nahi — ek bhi lakeer nahi, koi error bhi nahi. Bilkul yehi
        hua tha: phone par sirf aakhri jawab aata tha, jabke usi turn mein
        teen tools chal chuke hote thay.

        Wahi shart streaming par bhi lagti hai (SUPPORTS_MESSAGE_EDITING) —
        bina edit ke Hermes aadha paighaam bhejne se inkaar kar deta hai, aur
        theek karta hai: bina badle ja sakne wale aadhe paighaam se to poora
        paighaam behtar hai.

        Phone ki taraf ye ek `edit` event ban kar jata hai aur wahan usi id
        wala paighaam jagah par badal jata hai — koi nayi bubble nahi banti.
        Id `send()` ki di hui hoti hai, is liye dono taraf ek hi naam chalta
        hai.

        `finalize` yahan be-asar hai: hamari chat ka koi "abhi ban raha hai"
        wala alag roop nahi jo band karna pare (wo DingTalk jaise rich cards
        ke liye hai). Phir bhi phone tak bhej dete hain — sasta hai, aur us
        se wo jaan sakta hai ke ye aakhri tabdeeli thi.
        """
        if not self._http:
            return SendResult(success=False, error="Stonic bridge se rabta nahi hai")

        result = await self._post({
            "kind": "edit",
            "chat_id": str(chat_id or self._chat_id),
            "message_id": str(message_id),
            "text": content[:MAX_MESSAGE_LENGTH],
            "final": bool(finalize),
        })
        if not result.get("ok"):
            # `success=False` ka matlab Hermes ke liye "edit nahi ho saka" hai,
            # aur wo khud naya paighaam bhej kar sambhal leta hai. Is liye
            # yahan se throw karna kabhi theek nahi.
            return SendResult(success=False, error=str(result.get("error") or "bridge edit fail"))

        return SendResult(success=True, message_id=str(message_id))

    # ── Files: desktop ki disk se phone tak ─────────────────────────────────
    #
    # ════════════════════════════════════════════════════════════════════════
    #  YE TEEN METHOD HI POORA FEATURE HAIN
    # ════════════════════════════════════════════════════════════════════════
    #
    #  Hermes ka base adapter khud pehchan leta hai ke jawab mein kisi file ka
    #  raasta hai (gateway/platforms/base.py, `_non_image_local` waghera) aur
    #  us par ye methods bulata hai. Jo adapter inhein NAHI likhta, uske liye
    #  wo file ek saadi si text lakeer ban kar reh jati hai:
    #
    #      "📎 File: C:\\Users\\...\\report.pdf"
    #
    #  Yani phone par ek aisa raasta jo us par khulta hi nahi. Bilkul yehi
    #  pehle ho raha tha — user "wo file bhejo" kehta aur usay file ka pata
    #  milta, file nahi.
    #
    #  Inhein likh dene se poora silsila khud ba khud chal parta hai: agent ne
    #  jo file banai ya dhoondi, wo seedha phone par pohnch jati hai. Hum yahan
    #  file KHUD nahi bhejte — sirf uska raasta bridge ko dete hain, aur aage
    #  ka kaam (Supabase Storage par rakhna) desktop karta hai. Wajah saaf hai:
    #  ek 20MB ki file ko base64 bana kar HTTP par bhejna Hermes ke turn ko
    #  rok deta, jabke wo file bridge se do qadam door hi pari hai.

    async def _send_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> SendResult:
        """Ek file ka pata bridge tak. Asal upload desktop karta hai."""
        if not self._http:
            return SendResult(success=False, error="Stonic bridge se rabta nahi hai")

        message_id = uuid.uuid4().hex
        result = await self._post({
            "kind": "file",
            "chat_id": str(chat_id or self._chat_id),
            "path": str(file_path),
            "name": str(file_name) if file_name else "",
            "caption": (caption or "")[:MAX_MESSAGE_LENGTH],
            "message_id": message_id,
        })

        if not result.get("ok"):
            return SendResult(success=False, error=str(result.get("error") or "bridge file fail"))
        return SendResult(success=True, message_id=message_id)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, file_path, caption, file_name)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        # Tasveer aur document ka raasta ek hi hai — farq sirf phone par
        # dikhne ka hai, aur wo faisla wahan mime type se hota hai. Yahan do
        # alag raaste banane ka matlab ek hi cheez do jagah sambhalna hota.
        return await self._send_file(chat_id, image_path, caption)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, video_path, caption)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # Phone par "likh raha hai" wala nishan. Nakaam ho jaye to koi baat
        # nahi — ye sirf aaraam ke liye hai, kaam is par nahi ruka.
        await self._post({"kind": "typing", "chat_id": str(chat_id or self._chat_id)})

    async def _keep_typing(
        self,
        chat_id: str,
        interval: float = 2.0,
        metadata=None,
        stop_event: "asyncio.Event | None" = None,
    ) -> None:
        """Turn ka aaghaz aur anjaam — phone ko dono ki khabar deta hai.

        ── YE OVERRIDE KYUN HAI ────────────────────────────────────────────

        Hermes ek turn ke doran kai paighaam bhejta hai: tool ka progress,
        phir asli jawab. Phone ko ye jaanna zaroori hai ke turn KAB khatam
        hua — do wajah se:

          1. Screen par "kaam ho raha hai" tabhi tak dikhna chahiye
          2. Voice assistant ko jawab bol'na hai, aur usay pata hona chahiye
             ke ab aur kuch nahi aa raha

        `handle_message` is ke liye kaam nahi aata — wo foran wapas aa jata
        hai aur asal kaam ek background task mein hota hai. `_keep_typing`
        POORE turn ke doran chalta hai aur turn khatam hote hi cancel hota
        hai, is liye yehi wo do lamhe jaanne ki theek jagah hai. Hermes ke
        apne docs isi ko is qism ki cheez ke liye extension point kehte hain.

        `super()` ko bulana LAZMI hai, warna typing ka nishan chalta hi nahi.
        Aur `finally` zaroori hai: turn cancel ho kar khatam ho to bhi phone
        ko batana hai, warna wo hamesha "kaam ho raha hai" par atka rahega.
        """
        chat = str(chat_id or self._chat_id)
        if chat not in self._turns:
            self._turns.add(chat)
            await self._post({"kind": "turn", "chat_id": chat, "active": True})
        try:
            await super()._keep_typing(
                chat_id, interval=interval, metadata=metadata, stop_event=stop_event
            )
        finally:
            self._turns.discard(chat)
            # ── TURN-END KO ALAG TASK MEIN BHEJTE HAIN — JAAN BUJH KAR ──────
            #
            # Ye `finally` us waqt chalta hai jab gateway ne is task ko CANCEL
            # kiya hota hai (turn khatam hone par wo yehi karta hai). Aisi
            # aisi halat mein seedha await karna bharose ke laiq nahi —
            # cancel hoti hui task ka agla await khud bhi cancel ho sakta hai,
            # aur jo taraf hamara intezaar kar rahi hai us ka apna aadha
            # second ka timeout hai.
            #
            # Us surat mein turn-end ka paighaam kabhi bhejta hi nahi, aur
            # phone HAMESHA "kaam ho raha hai" par atka reh jata — jawab
            # saamne hote hue bhi. Ye us qism ki kharabi hai jo mahine baad,
            # kisi aur cheez ko dhoondte hue milti hai.
            #
            # Alag task cancel ke daayre se bahar hoti hai. Uska reference
            # rakhna zaroori hai: bina reference ke Python usay beech mein hi
            # utha (garbage collect kar) sakta hai.
            self._detach(self._post({"kind": "turn", "chat_id": chat, "active": False}))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": "Stonic Mobile", "type": "dm", "chat_id": chat_id}

    def _detach(self, coro) -> None:
        """Ek kaam ko mojooda task ke cancel se bahar bhej dena.

        Sirf turn-end ke liye — us ka pohnchna lazmi hai, chahe jis task ne
        usay shuru kiya wo mar rahi ho. Tafseel `_keep_typing` mein likhi hai.

        Loop hi band ho raha ho to nayi task ban hi nahi sakti. Us surat mein
        gateway waise bhi ja raha hai aur phone ka connection apne aap girega,
        is liye chup-chaap chhor dena theek hai.
        """
        try:
            task = asyncio.create_task(coro)
        except RuntimeError:
            coro.close()
            return
        self._detached.add(task)
        task.add_done_callback(self._detached.discard)

    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Bridge par ek paighaam. Kabhi throw nahi karta.

        Bridge ka na milna ek aam baat hai (desktop app band ho gayi, ya
        restart ho rahi hai). Us par exception phenkna Hermes ke turn ko maar
        deta — jabke masla sirf "aakhri qadam" ka hai. Is liye har kharabi
        yahan ek saada dict ban kar wapas jati hai.
        """
        if not self._http:
            return {"ok": False, "error": "bridge client not ready"}
        try:
            res = await self._http.post(
                f"{self._url}/v1/out",
                json=payload,
                headers={**_auth_headers(self._key), "Content-Type": "application/json"},
                timeout=15.0,
            )
            if res.status_code < 300:
                return {"ok": True}
            return {"ok": False, "error": f"HTTP {res.status_code}: {res.text[:200]}"}
        except Exception as e:  # noqa: BLE001 - har kharabi barabar hai
            return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
#  Plugin registration
# ═══════════════════════════════════════════════════════════════════════════


def check_requirements() -> bool:
    return HTTPX_AVAILABLE


def validate_config(pconfig) -> bool:
    extra = getattr(pconfig, "extra", {}) or {}
    url = extra.get("bridge_url") or _bridge_url()
    key = extra.get("bridge_key") or _bridge_key()
    return bool(url and key)


def is_connected(pconfig) -> bool:
    return validate_config(pconfig)


def _env_enablement() -> Optional[dict]:
    """Env vars se platform ko khud-ba-khud on karna.

    Stonic desktop app ye vars Hermes ke apne ``PUT /api/env`` se likhti hai.
    Un ke mojood hote hi platform config mein aa jata hai — user ko config.yaml
    kholne ki zaroorat nahi parti, aur ``hermes gateway status`` mein bhi nazar
    aa jata hai.
    """
    url = _bridge_url()
    key = _bridge_key()
    if not url or not key:
        return None

    chat_id = _default_chat_id()
    seed: dict = {"bridge_url": url, "bridge_key": key}
    if chat_id:
        seed["chat_id"] = chat_id

    home = (os.getenv("STONIC_HOME_CHANNEL", "") or "").strip() or chat_id
    if home:
        seed["home_channel"] = {"chat_id": home, "name": "Stonic Mobile"}
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Gateway ke bahar se bhejna — cron aur ``send_message`` tool ke liye.

    Jab ``hermes cron`` gateway se alag process mein chalta hai to koi zinda
    adapter mojood nahi hota. Is hook ke baghair ``deliver=stonic`` wala cron
    job chalta to hai magar paighaam "No live adapter" keh kar gir jata hai.

    Yahan hum ek waqti HTTP client se seedha bridge par bhej dete hain —
    bridge chal raha ho to paighaam phone tak pohnch jata hai.
    """
    if not HTTPX_AVAILABLE:
        return {"error": "stonic: httpx nahi mila"}

    extra = getattr(pconfig, "extra", {}) or {}
    url = (extra.get("bridge_url") or _bridge_url()).rstrip("/")
    key = extra.get("bridge_key") or _bridge_key()
    target = str(chat_id or extra.get("chat_id") or _default_chat_id() or "").strip()

    if not url or not key:
        return {"error": "stonic: bridge configured nahi hai (desktop app band hai?)"}
    if not target:
        return {"error": "stonic: chat id maloom nahi"}

    message_id = uuid.uuid4().hex
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                f"{url}/v1/out",
                json={
                    "kind": "message",
                    "chat_id": target,
                    "text": message[:MAX_MESSAGE_LENGTH],
                    "message_id": message_id,
                },
                headers={**_auth_headers(key), "Content-Type": "application/json"},
            )
            if res.status_code >= 300:
                return {"error": f"stonic bridge HTTP {res.status_code}: {res.text[:200]}"}

            # ── Files bhi, agar saath hon ────────────────────────────────
            #
            # Ye hissa cron ke liye hai. Gateway ke bahar koi zinda adapter
            # nahi hota, is liye upar wale `send_document` yahan chalte hi
            # nahi. Bina is ke "har raat report bana kar phone par bhejo"
            # jaisa job chalta to hai, magar phone par sirf text pohnchta hai
            # aur report peechhe reh jati hai — yani job ka asal maqsad hi
            # poora nahi hota.
            #
            # Har file apna alag paighaam banti hai (bilkul waise hi jaise
            # zinda adapter mein banti hai), aur ek file ka na jana baqi ko
            # nahi rokta.
            for media_path, _is_voice in (media_files or []):
                try:
                    file_res = await client.post(
                        f"{url}/v1/out",
                        json={
                            "kind": "file",
                            "chat_id": target,
                            "path": str(media_path),
                            "name": "",
                            "caption": "",
                            "message_id": uuid.uuid4().hex,
                        },
                        headers={**_auth_headers(key), "Content-Type": "application/json"},
                    )
                    if file_res.status_code >= 300:
                        logger.warning(
                            "stonic: file bridge tak nahi gayi (HTTP %s)", file_res.status_code
                        )
                except Exception as file_err:  # noqa: BLE001
                    logger.warning("stonic: file bhejne mein masla: %s", file_err)

        return {
            "success": True,
            "platform": PLATFORM_NAME,
            "chat_id": target,
            "message_id": message_id,
        }
    except Exception as e:  # noqa: BLE001
        return {"error": f"stonic standalone send fail: {e}"}


def register(ctx) -> None:
    """Plugin ka darwaza — Hermes ka plugin system boot par ise bulata hai."""
    # ── Live activity — desktop ki chat window ke liye ──────────────────────
    #
    # Platform se ALAG cheez hai, is liye pehle. Platform sirf phone ki chat
    # hai; ye hooks HAR us kaam ko dekhte hain jo is gateway process mein
    # chalta hai — phone se aayi command, Agent Town ke teammates, aur cron ke
    # waqt par chalne wale kaam — aur unka live progress desktop ki screen tak
    # pohnchate hain.
    #
    # Bridge na mile to ye khud-ba-khud khamosh rehte hain, is liye yahan koi
    # shart lagane ki zaroorat nahi. Tafseel: activity.py
    try:
        from . import activity

        activity.register_hooks(ctx)
    except Exception as err:  # noqa: BLE001
        # Live activity ek behtari hai, shart nahi. Iski nakaami par poora
        # plugin (yani phone ka poora rabta) nahi girna chahiye.
        logger.warning("Stonic live activity hooks nahi lag sake: %s", err)

    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Stonic Mobile",
        adapter_factory=lambda cfg: StonicAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["STONIC_BRIDGE_URL", "STONIC_BRIDGE_KEY"],
        install_hint="httpx pehle se Hermes ki dependency hai — kuch install nahi karna.",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="STONIC_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="STONIC_ALLOWED_USERS",
        allow_all_env="STONIC_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📱",
        # Stonic ki pehchan ek Supabase user id hai — koi phone number ya
        # email nahi jise logs mein chhupana pare.
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are talking to the user through the Stonic mobile app. "
            "The app renders full Markdown, including fenced code blocks, "
            "tables and links — use them freely. "
            "The user is on a PHONE: keep answers tight and scannable, put the "
            "answer first and the detail after, and avoid very wide tables. "
            "You are running on the user's own desktop computer, so you can act "
            "on their files, apps and browser — the phone is only the remote "
            "control they are speaking through. "
            # Ye do jumle bilkul zaroori hain. Agent ko file bhejne ki
            # salahiyat mil chuki hai, magar wo us se KHUD nahi jaanta ke
            # mil chuki hai — aur jo salahiyat wo apni nahi samajhta, use
            # kabhi istemal nahi karta. Bina in jumlon ke wo "ye file mere
            # paas hai, aap desktop par dekh lein" kehta rehta, jabke file
            # bhejna ab ek line ka kaam hai.
            "You CAN send files to the phone: when the user asks for a file, "
            "a screenshot, a document or an image that exists on this computer, "
            "put its absolute path in your reply and it is delivered to their "
            "phone as a real attachment they can open and save. Files up to "
            "25 MB work; for anything larger, compress it or send a link instead."
        ),
    )
