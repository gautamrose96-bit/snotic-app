"""Stonic Computer — Agent S3 ka sidecar.

Ye process Stonic desktop app (Electron) ke andar se chalta hai aur screen par
insaan ki tarah mouse/keyboard chalata hai. Dimagh Agent S3 ka hai
(``gui-agents`` package, bilkul wohi jo ``agent-s-lab`` ke ``start.bat`` se
chalta hai) — farq sirf itna ke terminal ke ``Query:`` prompt ki jagah kaam
Electron se aata hai.

Zindagi (resident service — Hermes ki tarah):

    Process app ke saath uthta hai, agent ka dimagh (gui_agents, ~2700 modules)
    USI WAQT main thread par load hota hai (stdin parhne se PEHLE — wajah
    Runner._stdin_lines par), aur process app band hone tak zinda rehta hai.
    ``ready`` sirf load ke BAAD jata hai — is se pehle Electron koi task nahi
    bhejta. Load ke dauran har 3 s ``loading`` ki dhadkan.

    "Stop" task rokta hai, process nahi: chalti hui cloud call kaat di jati hai
    (dekho _abort_llm_calls), cursor foran wapas, aur agent loaded ka loaded
    rehta hai — agla task foran shuru. Pehle stop = process ka qatal tha, aur
    har agle task par 26-500 s ka dobara load.

Protocol — NDJSON, ek line ek paigham:

    stdin  (Electron -> yahan)
        {"cmd":"run","id":"t1","task":"open notepad and type hello","max_steps":50,
         "overlay":true,"fast":false}
        {"cmd":"stop","id":"t1"}
        {"cmd":"ping"}
        {"cmd":"shutdown"}

    stdout (yahan -> Electron)
        {"event":"booting","screen":{...},"pid":123}          # process zinda, load shuru
        {"event":"loading","elapsed":12.0,"stage":"loading agent"}   # har 3 s
        {"event":"ready","screen":{"width":1920,"height":1080},"seconds":4.2}  # agent loaded
        {"event":"load_error","message":"..."}                # phir process exit 2
        {"event":"task_started","id":"t1"}
        {"event":"step","id":"t1","step":3,"max_steps":50,"thought":"...","action":"..."}
        {"event":"stopping","id":"t1"}
        {"event":"task_done","id":"t1","status":"done","steps":7,"summary":"..."}
        {"event":"pong","busy":false,"loaded":true}
        {"event":"error","message":"..."}

``status`` ki qeemat: done | failed | stopped | interrupted | max_steps | error.

Ek waqt mein sirf EK task. Doosra aaye to ``error`` ke saath rad hota hai —
mouse ek hi hai.

Config sirf environment se aati hai (Electron spawn par deta hai):

    STONIC_COMPUTER_API_URL          OpenAI-compatible base URL (gateway ya OpenRouter)
    STONIC_COMPUTER_API_KEY          us ke liye key
    STONIC_COMPUTER_PLANNER_MODEL    sochne wala model   (default openai/gpt-5.6-sol)
    STONIC_COMPUTER_GROUNDING_MODEL  x,y batane wala     (default bytedance/ui-tars-1.5-7b)
    STONIC_COMPUTER_GROUNDING_WIDTH / _HEIGHT   dev override; khali = screenshot ke naap se
    STONIC_COMPUTER_TESSERACT        tesseract.exe ka poora rasta (OCR ke liye)
    STONIC_COMPUTER_MAX_TRAJECTORY   kitne screenshots yaad rakhe (default 8)

Safety jo yahan hai:
  - pyautogui FAILSAFE: mouse screen ke top-left kone mein = foran band.
  - User ne khud mouse hilaya (steps ke darmiyan) = "interrupted", agent ruk jata hai.
  - ``stop`` command chalti hui cloud call ko kaat deti hai aur action ke
    andar ke sleep ko bhi — task ek-do second mein rukta hai, process zinda.
    Sirf asli hang par Electron process maarta hai (aur supervisor dobara uthata hai).
  - Overlay agent ke screenshots mein nazar nahi aata (WDA_EXCLUDEFROMCAPTURE).
"""

from __future__ import annotations

import ctypes
import io
import json
import logging
import math
import os
import platform
import re
import sys
import threading
import time
import traceback
import weakref
from typing import Any, Dict, List, Optional


# ── DPI awareness — pyautogui import se PEHLE ───────────────────────────────
#
# Windows par display scaling (125%, 150%) ke saath ek non-DPI-aware process
# ko screen chhoti dikhti hai: screenshot 1536x864 aata hai magar asal screen
# 1920x1080 hoti hai, aur har click ghalat jagah lagta hai. Per-monitor-v2
# awareness se screenshot aur click dono asal pixels mein hote hain.
def _make_dpi_aware() -> None:
    if platform.system() != "Windows":
        return
    try:
        user32 = ctypes.windll.user32
        user32.SetProcessDpiAwarenessContext.argtypes = [ctypes.c_void_p]
        # DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2 = -4
        if user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4)):
            return
    except Exception:
        pass
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)  # PROCESS_PER_MONITOR_DPI_AWARE
    except Exception:
        pass


_make_dpi_aware()

import pyautogui  # noqa: E402
from PIL import Image  # noqa: E402

pyautogui.FAILSAFE = True

# Overlay — Stonic ka apna hissa. Na mile to agent normal chalta hai.
try:
    from stonic_overlay import AgentOverlay, restore_system_cursors
except Exception:  # noqa: BLE001
    AgentOverlay = None

    def restore_system_cursors() -> bool:  # type: ignore[misc]
        return False


# ── Logging: sirf stderr par. stdout protocol ke liye mehfooz hai. ───────────
logging.basicConfig(
    level=os.environ.get("STONIC_COMPUTER_LOG_LEVEL", "INFO"),
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("stonic-computer")


# ── Config ───────────────────────────────────────────────────────────────────
def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


API_URL = _env("STONIC_COMPUTER_API_URL")
API_KEY = _env("STONIC_COMPUTER_API_KEY")
# Logical naam — gateway inhein asli model par mor deta hai. Electron hamesha
# ye khud bhejta hai; ye default sirf runner ko akele chalane ke liye hai.
PLANNER_MODEL = _env("STONIC_COMPUTER_PLANNER_MODEL", "stonic-computer-planner")
GROUNDING_MODEL = _env("STONIC_COMPUTER_GROUNDING_MODEL", "stonic-computer-grounding")
# Grounding ke naap JAAN BUJH KAR 0 hain — asli qeemat us tasveer se nikalti
# hai jo model ko bheji jati hai (dekho __init__). Agent S3 ka apna help text:
# "Width of screenshot image after processor rescaling". Yani ye model ki koi
# fixed khaasiyat nahi, us image ka naap hai jo usne dekhi. Pehle yahan 1920x1080
# likha tha — wo sirf 1080p screen par sach tha, jahan screenshot bina chhota
# hue usi naap ka rehta hai. 2K (2560x1440) par screenshot 2400x1350 jata tha
# magar model ko 1920x1080 bataya jata tha, to har coordinate 25% barh kar
# nikalta: click button se neeche-dayein khisak jata. Env sirf dev ka override.
GROUNDING_W = int(_env("STONIC_COMPUTER_GROUNDING_WIDTH", "0"))
GROUNDING_H = int(_env("STONIC_COMPUTER_GROUNDING_HEIGHT", "0"))
MAX_TRAJECTORY = int(_env("STONIC_COMPUTER_MAX_TRAJECTORY", "8"))
TESSERACT = _env("STONIC_COMPUTER_TESSERACT")

# Screenshot ka bara se bara rukh. 1920 is liye ke UI-TARS ke coordinates USI
# tasveer ke paimane mein aate hain jo usay bheji gayi, aur 1920x1080 wohi naap
# hai jis par ye pipeline asal mein sahi chalti hai (1080p laptop par clicks
# theek, aur 2K monitor ko 1920x1080 par laa kar bhi theek). Har display ko isi
# naap par laa dene se monitor par wohi accuracy milti hai jo laptop par hai.
# Lab mein 2400 tha — wahan screen 1920x1080 thi, to ye cap lagta hi nahi tha.
SCREENSHOT_MAX_DIM = 1920
# Step ke baad itni der intezaar (UI ko settle hone do) — lab mein bhi 1.0 tha.
POST_ACTION_WAIT_S = 1.0
# User ka mouse itne pixel hil jaye to "interrupted".
USER_MOUSE_MOVE_PX = 60
# Ek cloud call zyada se zyada itni der. OpenAI SDK ka default 10 MINUTE hai —
# ek atki hui call poore task ko utni der latka sakti thi.
LLM_TIMEOUT_S = float(_env("STONIC_COMPUTER_LLM_TIMEOUT_S", "120"))
# Stop ke baad worker thread ko apne aap khatam hone ke liye itna waqt; phir
# task ko "stopped" likh kar thread ko peeche chhor diya jata hai (abandoned).
STOP_JOIN_S = 4.0
# Naya task, pichhle (abandoned) thread ke mouse chhorne ka intezaar zyada se
# zyada itna kare — cloud call ka timeout is ke andar hai.
ABANDON_WAIT_S = LLM_TIMEOUT_S + 15.0


def _configure_tesseract() -> None:
    if not TESSERACT:
        return
    try:
        import pytesseract

        pytesseract.pytesseract.tesseract_cmd = TESSERACT
        # tessdata exe ke saath wale folder mein hai.
        os.environ.setdefault("TESSDATA_PREFIX", os.path.join(os.path.dirname(TESSERACT), "tessdata"))
    except Exception as err:  # noqa: BLE001
        log.warning("pytesseract configure nahi hua: %s", err)


# ── stdout par paigham ───────────────────────────────────────────────────────
class Emitter:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def send(self, event: str, **fields: Any) -> None:
        line = json.dumps({"event": event, **fields}, ensure_ascii=False)
        with self._lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()


emit = Emitter()


class TaskStopped(BaseException):
    """Electron ne ``stop`` bheja.

    JAAN BUJH KAR BaseException, Exception nahi: gui_agents ka har LLM raasta
    ``except Exception`` ke retry loops mein lipta hai (call_llm_safe 3 dafa,
    backoff 60 s tak). Exception hota to stop ka signal wahin nigal liya jata
    aur task chalta rehta. BaseException un sab se aar-paar seedha hamare
    ``_loop`` tak aata hai. (KeyboardInterrupt isi wajah se BaseException hai.)
    """


class UserInterrupted(Exception):
    """User ne khud mouse hilaya — machine wapas user ki."""


# ── Plan ki matn se insaan ke parhne wala hissa nikalna ─────────────────────
_SECTION_RE = re.compile(r"\((Previous action verification|Screenshot Analysis|Next Action|Grounded Action)\)", re.I)


def _plan_sections(plan: str) -> Dict[str, str]:
    """Agent S ke jawab ko uske chaar hisson mein todna."""
    out: Dict[str, str] = {}
    if not plan:
        return out
    parts = _SECTION_RE.split(plan)
    # parts = [pre, name1, body1, name2, body2, ...]
    for i in range(1, len(parts) - 1, 2):
        out[parts[i].lower()] = parts[i + 1].strip()
    return out


def _clip(text: str, n: int) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


def _scaled_size(width: int, height: int, max_dim: int) -> tuple[int, int]:
    factor = min(max_dim / width, max_dim / height, 1)
    return int(width * factor), int(height * factor)


# ── Cloud gateway ki rukawatein ───────────────────────────────────────
#
# Gateway (`/v1/computer/chat/completions`) sirf models ka proxy nahi — wohi
# ek jagah hai jahan plan ka gate lagta hai aur har step ka kharcha user ke
# daily/monthly quota se katta hai. Yani "quota khatam" ek AAM surat hai, koi
# anhoni nahi, aur usay do jagah theek se sambhalna parta hai.
#
# ── 1. 60 second ka bekaar intezaar ────────────────────────────────────
#
# Agent S ka har `generate` `backoff.on_exception(..., APIError, max_time=60)`
# se lipta hua hai. `APIError` OpenAI SDK mein har HTTP ghalti ka baap hai —
# yani 402 "plan khatam" par bhi wo poore 60 second tak dobara koshish karta
# rehta hai. Aisi ghalti par intezaar ka koi faida nahi: 60 second baad bhi
# plan khatam hi rahega. User ke liye ye sirf ek lamba sannata hai.
#
# Is liye wohi decorator dobara lagate hain, magar ek `giveup` shart ke saath.
# Asal (bin-lipta) function `__wrapped__` se milta hai — `functools.wraps` usay
# wahin chhor jata hai. Retry ka nizam waisa hi rehta hai; sirf wo ghaltiyan
# bahar nikal aati hain jo intezaar se theek hoti hi nahi.
#
# ── 2. Message ───────────────────────────────────────────────────
#
# Aur us ke baad user ko "APIStatusError: Error code: 402 - {...}" dikhana
# be-faida hai. `_friendly_error` usay us ek line mein badalta hai jo banda
# waqai parh kar samajh sake.

# Wo status jin par dobara koshish ka koi matlab nahi: request khud ghalat hai,
# key ghalat hai, ya plan/paisa khatam hai. In mein se koi bhi 60 second mein
# nahi badalta.
_PERMANENT_STATUS = {400, 401, 402, 403, 404}

# Quota ki hadd 429 par aati hai — magar wo reset ghanton baad hota hai, minton
# baad nahi. Qatar bhari hone wala 429 (limits.js) is se alag hai aur us par
# intezaar waqai kaam karta hai, is liye farq body ke matn se kiya jata hai.
_QUOTA_MARKERS = ("daily_limit", "monthly_limit", "insufficient_balance")

# 503 aam tor par "thori der baad theek ho jayega" hota hai, magar gateway ke ye
# do 503 config ki ghalti hain — wo intezaar se kabhi theek nahi hote.
_CONFIG_MARKERS = ("billing_not_configured", "upstream_not_configured")


def _error_body(err: Exception) -> str:
    """API ghalti ka jawab — jitna mil sake."""
    for attr in ("response", "body"):
        obj = getattr(err, attr, None)
        if obj is None:
            continue
        text = getattr(obj, "text", None)
        if isinstance(text, str) and text:
            return text
        if isinstance(obj, (str, dict)):
            return str(obj)
    return str(err)


def _giveup_on_permanent(err: Exception) -> bool:
    status = getattr(err, "status_code", None)
    if status in _PERMANENT_STATUS:
        return True
    body = _error_body(err).lower()
    if status == 429:
        return any(m in body for m in _QUOTA_MARKERS)
    if status == 503:
        return any(m in body for m in _CONFIG_MARKERS)
    return False


def _install_billing_fastfail() -> None:
    """Agent S ke engines par `giveup` shart lagao (upar wali wajah)."""
    try:
        import backoff
        from openai import APIConnectionError, APIError, RateLimitError
        from gui_agents.s3.core import engine as s3_engine
    except Exception as err:  # noqa: BLE001
        log.warning("billing fast-fail nahi laga (import): %s", err)
        return

    for name in ("LMMEngineOpenRouter", "LMMEngineOpenAI"):
        cls = getattr(s3_engine, name, None)
        raw = getattr(getattr(cls, "generate", None), "__wrapped__", None)
        if cls is None or raw is None:
            # Package ka dhancha badal gaya — chalne do, sirf retry lamba rahega.
            log.warning("billing fast-fail: %s par seam nahi mila", name)
            continue
        cls.generate = backoff.on_exception(
            backoff.expo,
            (APIConnectionError, APIError, RateLimitError),
            max_time=60,
            giveup=_giveup_on_permanent,
        )(raw)


def _friendly_error(err: Exception) -> Optional[str]:
    """Cloud ki rukawat ko aam zaban mein. Na pehchane to `None`."""
    status = getattr(err, "status_code", None)
    if status is None:
        return None
    body = _error_body(err).lower()

    if status == 401:
        return "Stonic Cloud sign-in ki muddat khatam ho gayi. App mein dobara sign in karein."
    if status == 403:
        return "Is account par Stonic Cloud band hai. Support se raabta karein."
    if status == 402 or (status == 429 and any(m in body for m in _QUOTA_MARKERS)):
        if "monthly_limit" in body:
            return "Is mahine ki usage limit khatam ho gayi. Computer use agle billing cycle par chalega."
        if "daily_limit" in body:
            return "Aaj ki usage limit khatam ho gayi. Computer use kuch ghanton baad phir chalega."
        return "Subscription active nahi ya balance khatam hai. Plan renew hone par computer use chalega."
    if status == 429:
        return "Cloud abhi masroof hai. Thori der baad dobara koshish karein."
    if status == 503 and any(m in body for m in _CONFIG_MARKERS):
        return "Computer use abhi cloud par set nahi hai (server config). Support ko batayein."
    if status >= 500:
        return "Cloud se jawab nahi mila. Thori der baad dobara koshish karein."
    return None


# ── Stop ko foran asar-daar banana: chalti hui cloud call kaatna ───────────
#
# Agent S ke engines apna OpenAI client khud banate hain — bina timeout ke
# (SDK ka default 10 minute). "Stop" pehle sirf steps ke DARMIYAN dekha jata
# tha, aur ek step 10-13 s ki cloud call hai. Is liye stop kabhi waqt par nahi
# rukta tha; Electron 8 s baad poora process maar deta (kal ke log mein 22 ke
# 22 stops isi raaste se gaye). Process marne ka matlab: loaded agent gaya,
# agla task phir 26-500 s ka load — user ko "Waking up the agent…" dobara.
#
# Ab do kaam:
#   1. Har engine ka client HUM banate hain (timeout ke saath) aur ek registry
#      mein rakhte hain — engine ka apna `if not self.llm_client` phir kabhi
#      nahi chalta.
#   2. Stop par registry ke saare clients band (close). Windows par chalti hui
#      socket recv usi lamhe toot jati hai. Aur un ki jagah _AbortedClient rakh
#      diya jata hai jo har AGLI call par TaskStopped phenkta hai — warna
#      call_llm_safe naya client bana kar dobara koshish kar leta.
#   Naye task se pehle _revive_llm_clients() band clients ki jagah naye
#   rakhta hai (planner ke engines to har task par naye bante hi hain; grounding
#   ke poore process ki zindagi ke hain).
_engines: "weakref.WeakSet[Any]" = weakref.WeakSet()
_engines_lock = threading.Lock()


class _AbortedClient:
    """Band kiye gaye client ki jagah: har istemal par TaskStopped."""

    def __getattr__(self, name: str) -> Any:
        raise TaskStopped()


def _make_client(engine: Any) -> Any:
    import httpx
    from openai import OpenAI

    return OpenAI(
        base_url=engine.base_url,
        api_key=engine.api_key,
        timeout=httpx.Timeout(LLM_TIMEOUT_S, connect=15.0),
        max_retries=1,
    )


def _install_abortable_clients() -> None:
    """Engines ke __init__ ko lapet do: client hamara, registry mein darj."""
    try:
        from gui_agents.s3.core import engine as s3_engine
    except Exception as err:  # noqa: BLE001
        log.warning("abortable clients nahi lage (import): %s", err)
        return

    for name in ("LMMEngineOpenRouter", "LMMEngineOpenAI"):
        cls = getattr(s3_engine, name, None)
        if cls is None or getattr(cls, "_stonic_abortable", False):
            continue
        orig_init = cls.__init__

        def __init__(self: Any, *args: Any, _orig: Any = orig_init, **kwargs: Any) -> None:
            _orig(self, *args, **kwargs)
            if getattr(self, "base_url", None) and getattr(self, "api_key", None):
                self.llm_client = _make_client(self)
            with _engines_lock:
                _engines.add(self)

        cls.__init__ = __init__
        cls._stonic_abortable = True


def _abort_llm_calls() -> int:
    """Chalti hui cloud calls kaato; agli calls foran TaskStopped dein."""
    with _engines_lock:
        engines = list(_engines)
    closed = 0
    for eng in engines:
        client = getattr(eng, "llm_client", None)
        eng.llm_client = _AbortedClient()
        if client is not None and not isinstance(client, _AbortedClient):
            try:
                client.close()
                closed += 1
            except Exception:  # noqa: BLE001
                pass
    return closed


def _revive_llm_clients() -> None:
    """Naye task se pehle: band kiye gaye clients ki jagah naye."""
    with _engines_lock:
        engines = list(_engines)
    for eng in engines:
        if isinstance(getattr(eng, "llm_client", None), _AbortedClient):
            ok = getattr(eng, "base_url", None) and getattr(eng, "api_key", None)
            eng.llm_client = _make_client(eng) if ok else None


# Agent ke actions ka code `import time; time.sleep(8)` jaisa hota hai
# (agent.wait). Wo sleep stop ko nahi dekhta — 10 s ka wait 10 s hi rukta.
# Chalane se pehle `time.sleep(` ko hamare rukne-wale sleep se badal dete hain.
_SLEEP_RE = re.compile(r"\btime\.sleep\(")


def _interruptible_sleep(stop: threading.Event):
    def _sleep(seconds: float) -> None:
        if stop.wait(max(0.0, float(seconds))):
            raise TaskStopped()

    return _sleep


class Task:
    """Ek task ka poora hisaab — apna stop signal, apna thread."""

    def __init__(self, task_id: str, text: str, max_steps: int, overlay: bool, fast: bool) -> None:
        self.id = task_id
        self.text = text
        self.max_steps = max_steps
        self.overlay = overlay
        self.fast = fast
        # Har task ka APNA stop. Ek mushtarka Event kaafi nahi: stop ke baad
        # peeche chhora hua thread agle task ke `clear()` par dobara chal parta.
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.steps = 0
        self.abandoned = False
        self._done_sent = False
        self._lock = threading.Lock()

    def mark_done(self) -> bool:
        """True sirf pehli dafa — task_done ek hi baar jata hai."""
        with self._lock:
            if self._done_sent:
                return False
            self._done_sent = True
            return True


# ── Agent ────────────────────────────────────────────────────────────────────
class ComputerAgent:
    """Agent S3 ko ek dafa banata hai aur tasks chalata hai."""

    def __init__(self) -> None:
        if not API_URL or not API_KEY:
            raise RuntimeError("STONIC_COMPUTER_API_URL / STONIC_COMPUTER_API_KEY set nahi hain.")

        _configure_tesseract()

        from gui_agents.s3.agents.agent_s import AgentS3
        from gui_agents.s3.agents.grounding import OSWorldACI

        # Quota/auth wali ghaltiyon par 60 second ka bekaar retry band karo.
        _install_billing_fastfail()
        # Stop par chalti hui cloud call kaati ja sake — engines banne se PEHLE.
        _install_abortable_clients()

        self.screen_w, self.screen_h = pyautogui.size()
        self.scaled_w, self.scaled_h = _scaled_size(self.screen_w, self.screen_h, SCREENSHOT_MAX_DIM)
        self.platform = platform.system().lower()

        # Dono model ek hi OpenAI-compatible darwaze se — lab ka `open_router`
        # engine yehi karta hai (base_url + api_key, aur kuch nahi).
        planner = {
            "engine_type": "open_router",
            "model": PLANNER_MODEL,
            "base_url": API_URL,
            "api_key": API_KEY,
            "temperature": None,
        }
        grounding = {
            "engine_type": "open_router",
            "model": GROUNDING_MODEL,
            "base_url": API_URL,
            "api_key": API_KEY,
            # Wohi naap jo `obs["screenshot"]` ka hai (dekho _loop) — is se
            # OSWorldACI.resize_coordinates ka hisaab
            # `coord * screen / screenshot` ban jata hai, jo har resolution aur
            # har aspect ratio par theek hai. 1080p par dono 1920x1080 hi nikalte
            # hain, yani wahan behaviour bilkul pehle jaisa.
            "grounding_width": GROUNDING_W or self.scaled_w,
            "grounding_height": GROUNDING_H or self.scaled_h,
        }

        # env=None: code agent (arbitrary Python/Bash) JAAN BUJH KAR band hai.
        # Terminal ka kaam Hermes ke paas pehle se hai; yahan sirf GUI.
        self.grounding_agent = OSWorldACI(
            env=None,
            platform=self.platform,
            engine_params_for_generation=planner,
            engine_params_for_grounding=grounding,
            width=self.screen_w,
            height=self.screen_h,
        )
        self._patch_text_coords()
        self.agent = AgentS3(
            planner,
            self.grounding_agent,
            platform=self.platform,
            max_trajectory_length=MAX_TRAJECTORY,
            enable_reflection=True,
        )

        # Agent ka banaya hua code isi namespace mein chalta hai (dekho _loop).
        # `pyautogui` aur `time` pehle se maujood hain kyunki har action ka code
        # unhein khud import karta hai — magar rakhne se pehla step ek import
        # bacha leta hai, aur ye namespace saaf bhi rehta hai: yahan sirf wohi
        # hai jo agent ko chahiye, poora module nahi.
        self._exec_ns: Dict[str, Any] = {"__builtins__": __builtins__, "pyautogui": pyautogui, "time": time}
        log.info(
            "Agent tayyar: screen %dx%d, planner=%s, grounding=%s",
            self.screen_w, self.screen_h, PLANNER_MODEL, GROUNDING_MODEL,
        )

    # ── OCR wale coords ka naap durust karo ─────────────────────────────────
    #
    # `highlight_text_span` (text select karna) baaqi actions se alag chalta
    # hai: wo grounding model se nahi, Tesseract se poochta hai ke lafz kahan
    # hai. Aur us ke jawab par upstream `resize_coordinates` lagana BHOOL gaya
    # hai (gui_agents/s3/agents/grounding.py — baaqi 13 actions par lagta hai,
    # isi ek par nahi). Nateeja: OCR tasveer ke pixel deta hai aur wo seedhe
    # screen ke pixel maan kar mouse chala diya jata hai.
    #
    # 1080p par ye ghalti chhupi rehti hai kyunki tasveer aur screen ka naap
    # barabar hota hai. Jahan screenshot chhota hota hai (2K, 4K) wahan text
    # ki selection ghalat jagah lag jati hai.
    #
    # Yahan sirf wohi chhoot gaya step lagaya ja raha hai — koi naya hisaab
    # nahi. Naap barabar ho (1080p) to haath bhi nahi lagta.
    def _patch_text_coords(self) -> None:
        if (self.scaled_w, self.scaled_h) == (self.screen_w, self.screen_h):
            return
        aci = self.grounding_agent
        inner = aci.generate_text_coords
        sx = self.screen_w / self.scaled_w
        sy = self.screen_h / self.scaled_h

        def to_screen(phrase: str, obs: Dict[str, Any], alignment: str = "") -> List[int]:
            x, y = inner(phrase, obs, alignment=alignment)
            return [round(x * sx), round(y * sy)]

        aci.generate_text_coords = to_screen
        log.info("OCR coords ka naap durust: x*%.4f, y*%.4f", sx, sy)

    # ── ek task ─────────────────────────────────────────────────────────────
    def run(self, t: Task) -> Dict[str, Any]:
        # fast = reflection band (har step par ek LLM call kam, ~30% tez).
        self.agent.enable_reflection = not t.fast
        self.agent.reset()  # naya Worker, saaf yaadash — har task self-contained
        # Actions ke andar ka `time.sleep` is task ke stop ko dekhe.
        self._exec_ns["_stonic_sleep"] = _interruptible_sleep(t.stop)

        overlay = None
        started = time.time()
        steps_done = 0
        last_sections: Dict[str, str] = {}
        status = "max_steps"
        error: Optional[str] = None
        try:
            # Pehle khabar, phir cursor — HUD usi lamhe "Working" par aa jaye.
            emit.send("task_started", id=t.id, max_steps=t.max_steps)
            if t.overlay and AgentOverlay is not None:
                try:
                    overlay = AgentOverlay().start()
                except Exception as err:  # noqa: BLE001
                    log.warning("Overlay nahi chala: %s", err)
            status = self._loop(t)
        except TaskStopped:
            status = "stopped"
        except UserInterrupted:
            status = "interrupted"
        except pyautogui.FailSafeException:
            status = "stopped"
            error = "failsafe: mouse top-left corner"
        except Exception as err:  # noqa: BLE001
            status = "error"
            # Cloud ki rukawat (quota, sign-in) aam si baat hai — usay raw
            # stack trace ki tarah nahi, seedhi baat ki tarah dikhao.
            error = _friendly_error(err) or f"{type(err).__name__}: {err}"
            log.error("Task crash: %s\n%s", err, traceback.format_exc())
        finally:
            if overlay is not None:
                try:
                    overlay.stop()
                except Exception:  # noqa: BLE001
                    pass
            steps_done = self.agent.executor.turn_count
            if self.agent.executor.worker_history:
                last_sections = _plan_sections(self.agent.executor.worker_history[-1])

        summary = self._summary(status, steps_done, last_sections, error)
        return {
            "id": t.id,
            "status": status,
            "steps": steps_done,
            "seconds": round(time.time() - started, 1),
            "summary": summary,
            "notes": list(self.grounding_agent.notes or []),
            "error": error,
        }

    def _loop(self, t: Task) -> str:
        obs: Dict[str, Any] = {}
        agent_mouse: Optional[tuple[int, int]] = None

        for step in range(1, t.max_steps + 1):
            if t.stop.is_set():
                raise TaskStopped()
            # User ne mouse hilaya? (pichle action ke baad se)
            if agent_mouse is not None:
                x, y = pyautogui.position()
                if math.hypot(x - agent_mouse[0], y - agent_mouse[1]) > USER_MOUSE_MOVE_PX:
                    raise UserInterrupted()

            shot = pyautogui.screenshot().resize((self.scaled_w, self.scaled_h), Image.LANCZOS)
            buf = io.BytesIO()
            shot.save(buf, format="PNG")
            obs["screenshot"] = buf.getvalue()

            info, code = self.agent.predict(instruction=t.text, observation=obs)
            action = (code[0] if code else "") or ""
            sections = _plan_sections(info.get("plan", ""))
            t.steps = step
            emit.send(
                "step",
                id=t.id,
                step=step,
                max_steps=t.max_steps,
                thought=_clip(sections.get("next action", ""), 200),
                action=_clip(info.get("plan_code", "") or action, 160),
            )

            marker = action.strip().upper()
            if marker == "DONE":
                return "done"
            if marker == "FAIL":
                return "failed"

            # Cloud call ke dauran stop aaya ho to yahan pakra jata hai —
            # action CHALNE SE PEHLE. Ek click bhi stop ke baad nahi lagta.
            if t.stop.is_set():
                raise TaskStopped()

            # Wohi exec jo lab ka cli_app karta hai — farq sirf ek: wahan ye
            # module ke darje par chalta hai, yahan ek method ke andar. Method
            # ke andar bina namespace diye exec ke paas do alag dictionaries
            # hoti hain (globals aur frame ke locals), aur us soorat mein
            # multi-line code — jaise `type` action ka try/except wala
            # pyperclip block — apne hi bandhe hue naam kabhi kabhi nahi dekh
            # pata. Ek hi dict de dene se ye theek wohi mahol ban jata hai jo
            # module ke darje par hota hai.
            exec(_SLEEP_RE.sub("_stonic_sleep(", action), self._exec_ns)  # noqa: S102 — sirf ACI ka banaya hua code
            if t.stop.wait(POST_ACTION_WAIT_S):
                raise TaskStopped()
            agent_mouse = tuple(pyautogui.position())

        return "max_steps"

    @staticmethod
    def _summary(status: str, steps: int, sections: Dict[str, str], error: Optional[str]) -> str:
        head = {
            "done": f"Task completed in {steps} steps.",
            "failed": f"Agent gave up after {steps} steps — it judged the task impossible from the screen.",
            "stopped": f"Task stopped by request after {steps} steps.",
            "interrupted": f"Task paused after {steps} steps because the user took the mouse.",
            "max_steps": f"Step limit reached after {steps} steps; the task may be incomplete.",
            "error": f"Task aborted after {steps} steps: {error or 'unknown error'}.",
        }.get(status, status)
        verification = _clip(sections.get("previous action verification", ""), 300)
        screen = _clip(sections.get("screenshot analysis", ""), 400)
        parts = [head]
        if verification:
            parts.append(f"Last check: {verification}")
        if screen:
            parts.append(f"Final screen: {screen}")
        return " ".join(parts)


# ── Main loop ────────────────────────────────────────────────────────────────
class Runner:
    """Resident sidecar: boot par load, task par task, stop par sirf task rukta hai."""

    def __init__(self) -> None:
        self._agent: Optional[ComputerAgent] = None
        self._lock = threading.Lock()
        # Agent banane ka apna alag taala. `_lock` is kaam ke liye theek nahi:
        # wo `_current` ke liye hai aur main thread (stdin) usay chhoota hai —
        # us par 150 second ka import baithane se poora protocol jam jata.
        self._agent_lock = threading.Lock()
        self._loaded = threading.Event()
        self._current: Optional[Task] = None
        # Stop ke baad bhi jis ka thread zinda ho (cloud call/action ke beech
        # tha). Agla task pehle is ke khatam hone ka intezaar karta hai —
        # mouse ek hai, do thread use nahi chala sakte.
        self._abandoned: Optional[Task] = None
        self._screen = pyautogui.size()

    # ── agent ka load (boot par, background mein) ────────────────────────────

    def _get_agent(self) -> ComputerAgent:
        # Taala is liye ke boot ka load aur (kisi wajah se pehle aa gaya) task
        # ek saath DO agent na banayein. Doosra bas intezaar karta hai — aur usi
        # dauran uski dhadkan Electron ko chalti rehti hai.
        with self._agent_lock:
            if self._agent is None:
                self._agent = ComputerAgent()
            return self._agent

    def _load_agent(self) -> None:
        """Boot par: agent load karo, dhadkan bhejo, phir `ready`.

        MAIN THREAD par chalta hai, stdin parhne se PEHLE — ye tarteeb hi asal
        fix hai (dekho _stdin_lines). Khali machine par 4-8 s. Load fail ho to
        process exit 2: Electron ka supervisor backoff ke saath dobara uthata hai.
        """
        t0 = time.time()

        def heartbeat() -> None:
            while not self._loaded.wait(3.0):
                emit.send("loading", elapsed=round(time.time() - t0, 1), stage="loading agent")

        threading.Thread(target=heartbeat, name="load-hb", daemon=True).start()
        try:
            self._get_agent()
            # Jo modules agent ke actions BAAD mein lazily import karte hain,
            # abhi le aao — task ke dauran koi naya DLL load na ho.
            try:
                import pyperclip  # noqa: F401  (`type` action clipboard se likhta hai)
            except Exception:  # noqa: BLE001
                pass
        except Exception as err:  # noqa: BLE001
            self._loaded.set()
            log.error("Agent load fail: %s\n%s", err, traceback.format_exc())
            emit.send("load_error", message=_friendly_error(err) or f"{type(err).__name__}: {err}")
            os._exit(2)
        self._loaded.set()
        secs = round(time.time() - t0, 1)
        log.info("Agent %s s mein tayyar", secs)
        w, h = self._screen
        emit.send("ready", screen={"width": w, "height": h}, pid=os.getpid(),
                  planner=PLANNER_MODEL, grounding=GROUNDING_MODEL, seconds=secs)

    # ── commands ─────────────────────────────────────────────────────────────

    def handle(self, msg: Dict[str, Any]) -> None:
        cmd = msg.get("cmd")
        if cmd == "ping":
            emit.send("pong", busy=self._current is not None, loaded=self._loaded.is_set())
        elif cmd == "warm":
            # Purana command — ab load boot par khud hota hai. Sirf halat batao.
            emit.send("warm", state="ready" if self._agent is not None else "loading")
        elif cmd == "run":
            self._start(msg)
        elif cmd == "stop":
            self._stop_current(msg.get("id"))
        elif cmd == "shutdown":
            self._shutdown()
        else:
            emit.send("error", message=f"unknown cmd: {cmd}")

    def _emit_done(self, t: Task, result: Dict[str, Any]) -> None:
        if t.mark_done():
            emit.send("task_done", **result)

    # ── stop: task rukta hai, process nahi ───────────────────────────────────

    def _stop_current(self, task_id: Optional[str]) -> None:
        with self._lock:
            t = self._current
            if t is None or (task_id and task_id != t.id):
                emit.send("error", message="no such task running", id=task_id)
                return
        t.stop.set()
        emit.send("stopping", id=t.id)
        # 1. Chalti hui cloud call kaato — worker thread usi lamhe TaskStopped
        #    par nikalta hai (cloud ka jawab aane ka intezaar nahi).
        closed = _abort_llm_calls()
        # 2. Cursor abhi wapas — worker ka apna cleanup bhi karega (idempotent).
        try:
            restore_system_cursors()
        except Exception:  # noqa: BLE001
            pass
        log.info("Stop: task %s, %d cloud client band", t.id, closed)
        # 3. Worker ko thora waqt do; na ruke to task yahin khatam, thread peeche.
        threading.Thread(target=self._await_stop, args=(t,), name=f"stop-{t.id}", daemon=True).start()

    def _await_stop(self, t: Task) -> None:
        th = t.thread
        if th is not None:
            th.join(STOP_JOIN_S)
        if th is None or not th.is_alive():
            return  # worker ne khud task_done bhej diya
        # Thread abhi bhi kisi call ya action mein atka hai. User ko intezaar
        # nahi karwana: task ab "stopped" hai, thread ko peeche chhor do. Wo
        # jab bhi nikle, `_loop` ka stop-check usay koi action chalane nahi
        # dega, aur uska task_done (mark_done) yahan ke baad nahi jayega.
        with self._lock:
            t.abandoned = True
            if self._current is t:
                self._current = None
            self._abandoned = t
        log.warning("Stop: task %s ka thread %ss mein nahi ruka — peeche chhor diya", t.id, STOP_JOIN_S)
        self._emit_done(t, {
            "id": t.id, "status": "stopped", "steps": t.steps, "seconds": 0,
            "summary": f"Task stopped by request after {t.steps} steps.",
            "notes": [], "error": None,
        })

    # ── run ──────────────────────────────────────────────────────────────────

    def _start(self, msg: Dict[str, Any]) -> None:
        task_id = str(msg.get("id") or f"task-{int(time.time() * 1000)}")
        text = str(msg.get("task") or "").strip()
        if not text:
            emit.send("error", id=task_id, message="task is empty")
            return
        t = Task(
            task_id, text,
            max_steps=max(1, min(int(msg.get("max_steps") or 50), 200)),
            overlay=bool(msg.get("overlay", True)),
            fast=bool(msg.get("fast", False)),
        )
        with self._lock:
            if self._current is not None:
                emit.send("error", id=task_id, message="busy", busy_with=self._current.id)
                return
            self._current = t

        def work() -> None:
            # Aam soorat mein agent boot par load ho chuka hai aur ye thread
            # seedha run par jata hai. Do soorat mein intezaar hota hai — aur
            # dono mein har 3 s dhadkan jati hai taake Electron (aur HUD) ko
            # pata rahe ke zinda hain:
            #   - agent abhi load ho raha ho (task boot se pehle aa gaya)
            #   - pichhla task stop hua magar uska thread abhi mouse chhor raha ho
            waiting = threading.Event()
            stage = ["loading agent"]

            def heartbeat() -> None:
                t0 = time.time()
                while not waiting.wait(3.0):
                    emit.send("task_starting", id=t.id, elapsed=round(time.time() - t0, 1), stage=stage[0])

            threading.Thread(target=heartbeat, name=f"hb-{t.id}", daemon=True).start()
            try:
                prev = self._abandoned
                if prev is not None and prev.thread is not None and prev.thread.is_alive():
                    stage[0] = "waiting for previous task to release the mouse"
                    prev.thread.join(ABANDON_WAIT_S)
                    if prev.thread.is_alive():
                        raise RuntimeError("previous task is still releasing the mouse; try again in a moment")
                self._abandoned = None
                agent = self._get_agent()
                waiting.set()
                _revive_llm_clients()
                result = agent.run(t)
            except Exception as err:  # noqa: BLE001
                log.error("Agent init/run fail: %s\n%s", err, traceback.format_exc())
                result = {
                    "id": t.id, "status": "error", "steps": 0, "seconds": 0,
                    "summary": f"Computer agent could not start: {err}",
                    "notes": [], "error": str(err),
                }
            finally:
                waiting.set()
                with self._lock:
                    if self._current is t:
                        self._current = None
            self._emit_done(t, result)

        t.thread = threading.Thread(target=work, name=f"task-{t.id}", daemon=True)
        t.thread.start()

    # ── shutdown ─────────────────────────────────────────────────────────────

    def _shutdown(self) -> None:
        with self._lock:
            t = self._current
        if t is not None:
            t.stop.set()
        _abort_llm_calls()
        try:
            restore_system_cursors()
        except Exception:  # noqa: BLE001
            pass
        emit.send("bye")
        os._exit(0)

    # ── stdin: kabhi ReadFile mein park na ho ────────────────────────────────
    #
    # 13 Sept ko py-spy se pakri hui asal jar. Windows par agar koi thread stdin
    # pipe par `ReadFile` mein para ho aur usi waqt koi DOOSRA thread numpy ki
    # OpenBLAS DLL load kare (import numpy → _multiarray_umath), to wo load ek
    # CRT lock par hamesha ke liye atak jata hai. Process ka exit bhi ruk jata
    # hai. Ye tab hi khulta tha jab stdin par koi line aati (lock ek lamhe ko
    # chhootta) — isi liye app mein agent 26-500 s mein load hota tha aur "stop
    # kar ke dobara lagao" se kabhi kabhi chal parta tha: har command ek dhakka.
    #
    # Do tehain:
    #   1. Agent ka load MAIN thread par, stdin parhne se PEHLE (serve dekho).
    #   2. stdin ko PeekNamedPipe se dekh kar parho — data ho to hi os.read.
    #      Koi thread kabhi ReadFile mein park nahi hota, to task ke dauran
    #      lazy import hone wala koi bhi DLL bhi mehfooz hai.
    # Tajurba: probe mein ReadFile-park + thread load = hang (5/5), polling =
    # 5.8 s mein load. Console (pipe nahi) par purana raasta hi chalta hai.

    @staticmethod
    def _stdin_lines(poll_s: float = 0.05):
        if platform.system() != "Windows":
            yield from sys.stdin
            return
        try:
            import msvcrt
            from ctypes import wintypes

            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = wintypes.HANDLE(msvcrt.get_osfhandle(0))
            if k32.GetFileType(handle) != 3:  # FILE_TYPE_PIPE — console/file par purana raasta
                yield from sys.stdin
                return
        except Exception:  # noqa: BLE001
            yield from sys.stdin
            return

        avail = wintypes.DWORD(0)
        pending = b""
        while True:
            if not k32.PeekNamedPipe(handle, None, 0, None, ctypes.byref(avail), None):
                break  # pipe toot gayi = Electron chala gaya
            if avail.value == 0:
                time.sleep(poll_s)
                continue
            try:
                chunk = os.read(0, min(avail.value, 65536))
            except OSError:
                break
            if not chunk:
                break
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                yield line.decode("utf-8", "replace")
        if pending.strip():
            yield pending.decode("utf-8", "replace")

    def serve(self) -> None:
        w, h = self._screen
        emit.send("booting", screen={"width": w, "height": h}, pid=os.getpid(),
                  planner=PLANNER_MODEL, grounding=GROUNDING_MODEL)
        # Pehle load, phir stdin — is tarteeb ki wajah upar (_stdin_lines).
        # Load ke dauran aaye commands pipe mein intezaar karte hain; Electron
        # waise bhi `ready` se pehle `run` nahi bhejta.
        self._load_agent()
        for raw in self._stdin_lines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                emit.send("error", message="invalid json")
                continue
            try:
                self.handle(msg)
            except Exception as err:  # noqa: BLE001
                log.error("handle fail: %s\n%s", err, traceback.format_exc())
                emit.send("error", message=str(err))
        # stdin band = Electron chala gaya. Kuch bhi peeche na rahe.
        self._shutdown()


if __name__ == "__main__":
    Runner().serve()
