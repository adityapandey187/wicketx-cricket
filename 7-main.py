"""
Live Cricket Score REST API (self-hosted, no API keys, zero cost).

Scrapes Cricbuzz's public pages (live scores, recent, upcoming, scorecards),
parses the match JSON embedded in each page (Next.js flight data), caches it in
RAM with TTLs, and serves it through FastAPI.

Run:  uvicorn main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

log = logging.getLogger("cricket-api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = "https://www.cricbuzz.com"
URL_LIVE = BASE + "/cricket-match/live-scores"
URL_RECENT = BASE + "/cricket-match/live-scores/recent-matches"
URL_UPCOMING = BASE + "/cricket-match/live-scores/upcoming-matches"

TTL_LIVE = 30          # seconds: live matches (and per-match live details)
TTL_SCORECARD = 30     # seconds: scorecards
TTL_OTHER = 300        # seconds: completed / upcoming lists

MAX_DETAIL_MATCHES = 12      # live matches enriched with batsmen/bowler per list call
BACKGROUND_INTERVAL = 30     # seconds between background refresh passes
ACTIVE_WINDOW = 180          # background refresh only if a client asked in the last N seconds

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]


class ScrapeError(Exception):
    """Raised when upstream fetching or parsing fails."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return iso(time.time())


def num_str(v: Any) -> Optional[str]:
    """Number -> compact string ('106.3', '434'); None stays None."""
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


# --------------------------------------------------------------------------- #
# Async TTL cache with stale fallback, request coalescing and backoff
# --------------------------------------------------------------------------- #
class TTLCache:
    def __init__(self) -> None:
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._fail_count: Dict[str, int] = {}
        self._retry_after: Dict[str, float] = {}

    def _lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def peek(self, key: str) -> Optional[Tuple[float, Any]]:
        return self._data.get(key)

    def is_fresh(self, key: str, ttl: float) -> bool:
        item = self._data.get(key)
        return item is not None and (time.time() - item[0]) < ttl

    async def get(
        self, key: str, ttl: float, loader: Callable[[], Awaitable[Any]]
    ) -> Tuple[Any, float, bool]:
        """Return (value, fetched_at, stale). Raises ScrapeError only if nothing is cached."""
        item = self._data.get(key)
        if item and (time.time() - item[0]) < ttl:
            return item[1], item[0], False
        async with self._lock(key):
            # Another request may have refreshed while we waited for the lock.
            item = self._data.get(key)
            if item and (time.time() - item[0]) < ttl:
                return item[1], item[0], False
            # Backoff window after repeated failures: do not hammer upstream.
            if time.time() < self._retry_after.get(key, 0):
                if item:
                    return item[1], item[0], True
                raise ScrapeError("upstream temporarily unavailable (backing off)")
            try:
                value = await loader()
            except Exception as exc:  # noqa: BLE001 - any failure falls back to stale data
                n = self._fail_count.get(key, 0) + 1
                self._fail_count[key] = n
                self._retry_after[key] = time.time() + min(30 * (2 ** (n - 1)), 900)
                log.warning("refresh failed for %s (%s): %s", key, n, exc)
                if item:
                    return item[1], item[0], True
                raise ScrapeError(str(exc)) from exc
            self._fail_count[key] = 0
            self._retry_after.pop(key, None)
            self._data[key] = (time.time(), value)
            return value, self._data[key][0], False

    def prune(self, max_age: float = 3600) -> None:
        cutoff = time.time() - max_age
        for k in [k for k, (t, _) in self._data.items() if t < cutoff]:
            self._data.pop(k, None)


cache = TTLCache()
_state: Dict[str, Any] = {"client": None, "last_request": 0.0, "semaphore": None}


def touch() -> None:
    _state["last_request"] = time.time()


# --------------------------------------------------------------------------- #
# HTTP fetching
# --------------------------------------------------------------------------- #
def browser_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }


async def fetch_html(url: str) -> str:
    client: httpx.AsyncClient = _state["client"]
    if _state.get("upstream_blocked"):
        raise ScrapeError("Source refresh paused after a block")
    try:
        resp = await client.get(url, headers=browser_headers())
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise ScrapeError("upstream request failed") from exc
    if resp.status_code in (403, 429):
        _state["upstream_blocked"] = True
        raise ScrapeError(f"HTTP {resp.status_code}: source refresh paused")
    if resp.status_code != 200 or len(resp.text) < 5000:
        raise ScrapeError(f"HTTP {resp.status_code}: upstream unavailable")
    return resp.text



# --------------------------------------------------------------------------- #
# Parsing: Cricbuzz embeds its data as JSON inside Next.js flight <script> tags
# --------------------------------------------------------------------------- #
_PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', re.S)
_decoder = json.JSONDecoder()


def flight_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    chunks: List[str] = []
    for script in soup.find_all("script"):
        src = script.string or script.get_text() or ""
        if "__next_f.push" not in src:
            continue
        for m in _PUSH_RE.finditer(src):
            try:
                chunks.append(json.loads('"' + m.group(1) + '"'))
            except ValueError:
                continue
    text = "".join(chunks)
    if not text:
        raise ScrapeError("no embedded match data found in page (layout changed or blocked)")
    return text


def decode_at(text: str, pos: int) -> Tuple[Any, int]:
    return _decoder.raw_decode(text, pos)


def find_value(text: str, key: str, start: int = 0) -> Any:
    marker = f'"{key}":'
    i = text.find(marker, start)
    while i != -1:
        j = i + len(marker)
        if j < len(text) and text[j] in "{[":
            try:
                return decode_at(text, j)[0]
            except ValueError:
                pass
        i = text.find(marker, i + 1)
    return None


def extract_matches(text: str) -> Dict[int, Dict[str, Any]]:
    """All {matchInfo, matchScore} pairs in the page, de-duplicated by matchId."""
    out: Dict[int, Dict[str, Any]] = {}
    marker = '"matchInfo":'
    i = text.find(marker)
    while i != -1:
        j = i + len(marker)
        try:
            info, end = decode_at(text, j)
        except ValueError:
            i = text.find(marker, j)
            continue
        score = None
        tail = ',"matchScore":'
        if text.startswith(tail, end):
            try:
                score = decode_at(text, end + len(tail))[0]
            except ValueError:
                score = None
        if isinstance(info, dict) and "matchId" in info:
            mid = int(info["matchId"])
            prev = out.get(mid)
            if prev is None or (score and not prev.get("score")):
                out[mid] = {"info": info, "score": score}
        i = text.find(marker, j)
    if not out:
        raise ScrapeError("no matches found in page")
    return out


# ----- classification ------------------------------------------------------- #
WOMEN_KW = ("women", "wbbl", "wpl", "wcpl", "wodi", "(w)", " w vs", "girls")
LEAGUE_KW = (
    "indian premier league", "ipl", "big bash", "bbl", "pakistan super league", "psl",
    "caribbean premier league", "cpl", "ilt20", "international league t20", "sa20", "the hundred",
    "major league cricket", "mlc", "lanka premier league", "lpl", "bangladesh premier league",
    "bpl", "t10 league", "abu dhabi t10", "super smash", "global t20", "legends league",
    "t20 blast", "vitality blast", "ram slam", "lpl", "npl", "nepal premier",
)
DOMESTIC_KW = (
    "ranji", "vijay hazare", "syed mushtaq", "duleep", "deodhar", "irani cup", "county",
    "sheffield shield", "marsh", "ford trophy", "plunket", "csa t20 challenge", "csa 4-day",
    "one-day cup", "royal london", "t20 blast", "pro50", "premier league", "inter-provincial",
    "quaid-e-azam", "national t20 cup", "champions one-day", "domestic", "shield",
)
INTL_STRONG_KW = ("icc ", "world cup", "asia cup", "tour of", "tri-series", "u19 ", " u19", "u23", "t20i", "unofficial")


def classify(info: Dict[str, Any]) -> str:
    series = (info.get("seriesName") or "").lower()
    desc = (info.get("matchDesc") or "").lower()
    t1 = (info.get("team1") or {}).get("teamName", "").lower()
    t2 = (info.get("team2") or {}).get("teamName", "").lower()
    blob = f" {series} {desc} {t1} {t2} "
    if any(k in blob for k in WOMEN_KW):
        return "women"
    if any(k in blob for k in INTL_STRONG_KW):
        return "international"
    if any(re.search(r"\b" + re.escape(k) + r"\b", blob) for k in LEAGUE_KW):
        return "league"
    if any(k in blob for k in DOMESTIC_KW):
        return "domestic"
    # Cricbuzz's own matchType is only a fallback: it is missing or wrong for some series.
    mtype = (info.get("matchType") or "").lower()
    if mtype in ("league", "domestic", "international", "women"):
        return mtype
    return "international"


_INDIA_NAME = re.compile(r"\bindia\b", re.I)


def is_india_team(team: Dict[str, Any]) -> bool:
    name = team.get("teamName") or team.get("name") or ""
    short = (team.get("teamSName") or team.get("shortName") or "").upper()
    short_is_india = short.startswith("IND") and short[3:] in ("", "A", "W", "U19", "U23", "WA", "WU19", "-A", "-W")
    return bool(_INDIA_NAME.search(name)) or short_is_india


UPCOMING_STATES = {"preview", "upcoming"}
COMPLETE_STATES = {"complete", "completed", "abandon", "abandoned", "no result", "cancelled", "canceled"}


def map_status(info: Dict[str, Any]) -> str:
    state = (info.get("state") or "").strip().lower()
    if state in UPCOMING_STATES:
        return "upcoming"
    if state in COMPLETE_STATES or state.startswith("abandon"):
        return "complete"
    return "live"


def innings_score_str(inn_map: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if not inn_map:
        return None, None
    parts: List[str] = []
    overs: Optional[str] = None

    def order(k: str) -> int:
        m = re.search(r"(\d+)", k)
        return int(m.group(1)) if m else 0

    for key in sorted(inn_map.keys(), key=order):
        inn = inn_map[key]
        runs = inn.get("runs")
        wkts = inn.get("wickets")
        s = str(runs if runs is not None else 0)
        if wkts is not None and int(wkts) < 10:
            s += f"/{wkts}"
        if inn.get("isDeclared"):
            s += "d"
        parts.append(s)
        overs = num_str(inn.get("overs"))
    return " & ".join(parts), overs


def build_team(team: Dict[str, Any], score_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    score, overs = innings_score_str(score_obj or {})
    return {
        "name": team.get("teamName"),
        "short_name": team.get("teamSName"),
        "score": score,
        "overs": overs,
    }


def build_match(entry: Dict[str, Any]) -> Dict[str, Any]:
    info = entry["info"]
    score = entry.get("score") or {}
    t1, t2 = info.get("team1") or {}, info.get("team2") or {}
    venue = info.get("venueInfo") or {}
    venue_txt = ", ".join(x for x in (venue.get("ground"), venue.get("city")) if x) or None
    title_core = f"{t1.get('teamName', 'TBC')} vs {t2.get('teamName', 'TBC')}"
    desc = info.get("matchDesc")
    start = info.get("startDate")
    return {
        "id": str(info["matchId"]),
        "title": f"{title_core}, {desc}" if desc else title_core,
        "series": info.get("seriesName"),
        "category": classify(info),
        "status": map_status(info),
        "status_text": info.get("status"),
        "venue": venue_txt,
        "is_india_match": is_india_team(t1) or is_india_team(t2),
        "team_a": build_team(t1, score.get("team1Score")),
        "team_b": build_team(t2, score.get("team2Score")),
        "current_state": None,
        "match_format": info.get("matchFormat"),
        "start_time": iso(int(start) / 1000) if start else None,
    }


def parse_recent_balls(s: Optional[str]) -> List[str]:
    if not s:
        return []
    tokens = [t for t in re.split(r"[\s|]+", s) if t and t != "..."]
    return tokens[-6:]


def build_current_state(mini: Dict[str, Any]) -> Dict[str, Any]:
    def bat(b: Optional[Dict[str, Any]], strike: bool) -> Optional[Dict[str, Any]]:
        if not b:
            return None
        return {
            "name": b.get("name"), "runs": num_str(b.get("runs")), "balls": num_str(b.get("balls")),
            "fours": num_str(b.get("fours")), "sixes": num_str(b.get("sixes")), "is_strike": strike,
        }

    batsmen = [x for x in (bat(mini.get("batsmanStriker"), True), bat(mini.get("batsmanNonStriker"), False)) if x]
    bw = mini.get("bowlerStriker")
    bowler = None
    if bw:
        bowler = {
            "name": bw.get("name"), "overs": num_str(bw.get("overs")), "maidens": num_str(bw.get("maidens")),
            "runs": num_str(bw.get("runs")), "wickets": num_str(bw.get("wickets")),
        }
    crr = mini.get("currentRunRate")
    rrr = mini.get("requiredRunRate")
    target = mini.get("target")
    return {
        "crr": num_str(crr) if crr else None,
        "rrr": num_str(rrr) if rrr else None,
        "target": num_str(target) if target else None,
        "batsmen": batsmen,
        "bowler": bowler,
        "recent_balls": parse_recent_balls(mini.get("recentOvsStats")),
        "last_wicket": mini.get("lastWicket") if mini.get("lastWicket") not in (None, "$undefined") else None,
    }


def seq_values(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    def order(k: str) -> int:
        m = re.search(r"(\d+)$", k)
        return int(m.group(1)) if m else 0

    return [d[k] for k in sorted(d.keys(), key=order)]


def build_scorecard(text: str, match_id: str) -> Dict[str, Any]:
    cards = find_value(text, "scoreCard")
    header = find_value(text, "matchHeader") or {}
    if cards is None and not header:
        raise ScrapeError("scorecard data not found in page")
    t1 = (header.get("team1") or {}).get("name", "")
    t2 = (header.get("team2") or {}).get("name", "")
    state = (header.get("state") or "").lower()
    if header.get("complete"):
        status = "complete"
    elif state in UPCOMING_STATES:
        status = "upcoming"
    else:
        status = "live"
    innings: List[Dict[str, Any]] = []
    for inn in cards or []:
        bt = inn.get("batTeamDetails") or {}
        bowl = inn.get("bowlTeamDetails") or {}
        sd = inn.get("scoreDetails") or {}
        names: Dict[Any, str] = {}
        batting = []
        for b in seq_values(bt.get("batsmenData") or {}):
            names[b.get("batId")] = b.get("batName")
            out_desc = (b.get("outDesc") or "").strip()
            if not out_desc and not b.get("balls") and not b.get("runs"):
                continue  # yet to bat
            batting.append({
                "name": b.get("batName"),
                "dismissal": out_desc or "not out",
                "runs": b.get("runs", 0), "balls": b.get("balls", 0),
                "fours": b.get("fours", 0), "sixes": b.get("sixes", 0),
                "sr": b.get("strikeRate", 0),
            })
        bowling = [{
            "name": b.get("bowlName"), "overs": num_str(b.get("overs")), "maidens": b.get("maidens", 0),
            "runs": b.get("runs", 0), "wickets": b.get("wickets", 0), "economy": b.get("economy", 0),
        } for b in seq_values(bowl.get("bowlersData") or {})]
        fow = [{
            "score": f"{w.get('wktRuns')}/{w.get('wktNbr')}",
            "player": w.get("batName") or names.get(w.get("batId")),
            "over": num_str(w.get("wktOver")),
        } for w in seq_values(inn.get("wicketsData") or {})]
        extras = inn.get("extrasData") or {}
        innings.append({
            "team": bt.get("batTeamName"),
            "innings_number": inn.get("inningsId"),
            "score": num_str(sd.get("runs")),
            "wickets": num_str(sd.get("wickets")),
            "overs": num_str(sd.get("overs")),
            "extras": {
                "total": extras.get("total"), "byes": extras.get("byes"), "leg_byes": extras.get("legByes"),
                "wides": extras.get("wides"), "no_balls": extras.get("noBalls"), "penalty": extras.get("penalty"),
            } if extras else None,
            "batting": batting,
            "bowling": bowling,
            "fall_of_wickets": fow,
        })
    return {
        "match_id": str(match_id),
        "title": f"{t1} vs {t2}".strip() if t1 or t2 else None,
        "status": status,
        "status_text": header.get("status"),
        "innings": innings,
    }


# --------------------------------------------------------------------------- #
# Loaders (each one scrapes + parses and returns plain Python data)
# --------------------------------------------------------------------------- #
async def load_page_matches(url: str) -> List[Dict[str, Any]]:
    html = await fetch_html(url)
    text = await asyncio.to_thread(flight_text, html)
    return [build_match(e) for e in extract_matches(text).values()]


async def load_live_state(match_id: str) -> Optional[Dict[str, Any]]:
    html = await fetch_html(f"{BASE}/live-cricket-scores/{match_id}/x")
    text = await asyncio.to_thread(flight_text, html)
    mini = find_value(text, "miniscore")
    return build_current_state(mini) if isinstance(mini, dict) else None


async def load_scorecard(match_id: str) -> Dict[str, Any]:
    html = await fetch_html(f"{BASE}/live-cricket-scorecard/{match_id}/x")
    text = await asyncio.to_thread(flight_text, html)
    return build_scorecard(text, match_id)


# --------------------------------------------------------------------------- #
# India-first sorting
# --------------------------------------------------------------------------- #
_STATUS_RANK = {"live": 0, "complete": 1, "upcoming": 2}


def sort_matches(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """India matches first, then live, then recent (complete), then upcoming.

    Inside each group: live and recent newest first, upcoming soonest first.
    """

    def key(m: Dict[str, Any]) -> Tuple[int, int, float]:
        start = m.get("start_time") or ""
        ts = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ").timestamp() if start else 0.0
        within = ts if m["status"] == "upcoming" else -ts
        return (0 if m["is_india_match"] else 1, _STATUS_RANK[m["status"]], within)

    return sorted(matches, key=key)


# --------------------------------------------------------------------------- #
# Service layer
# --------------------------------------------------------------------------- #
async def get_all_matches() -> Tuple[List[Dict[str, Any]], float, bool]:
    """Merge live page (15s TTL) with recent/upcoming pages (5 min TTL)."""
    results = await asyncio.gather(
        cache.get("page:live", TTL_LIVE, lambda: load_page_matches(URL_LIVE)),
        cache.get("page:recent", TTL_OTHER, lambda: load_page_matches(URL_RECENT)),
        cache.get("page:upcoming", TTL_OTHER, lambda: load_page_matches(URL_UPCOMING)),
        return_exceptions=True,
    )
    good = [r for r in results if not isinstance(r, BaseException)]
    if not good:
        raise ScrapeError(str(results[0]))
    merged: Dict[str, Dict[str, Any]] = {}
    # Later entries are overridden by earlier ones: live page wins, then recent, then upcoming.
    for r in reversed(results):
        if isinstance(r, BaseException):
            continue
        for m in r[0]:
            merged[m["id"]] = dict(m)
    stale = any(r[2] for r in good) or len(good) < len(results)
    fetched = min(r[1] for r in good)
    return list(merged.values()), fetched, stale


async def enrich_live(matches: List[Dict[str, Any]]) -> bool:
    """Attach batsmen/bowler/recent balls to live matches (India first, capped). Returns stale flag."""
    live = [m for m in matches if m["status"] == "live"]
    live = sort_matches(live)[:MAX_DETAIL_MATCHES]
    sem: asyncio.Semaphore = _state["semaphore"]
    any_stale = False

    async def one(m: Dict[str, Any]) -> None:
        nonlocal any_stale
        async with sem:
            try:
                state, _, stale = await cache.get(
                    f"live:{m['id']}", TTL_LIVE, lambda: load_live_state(m["id"])
                )
            except ScrapeError:
                return
        any_stale = any_stale or stale
        m["current_state"] = state

    await asyncio.gather(*(one(m) for m in live))
    return any_stale


def filter_matches(matches: List[Dict[str, Any]], category: str, status: str) -> List[Dict[str, Any]]:
    out = matches
    if category != "all":
        out = [m for m in out if m["category"] == category]
    if status != "all":
        wanted = {"live": "live", "recent": "complete", "upcoming": "upcoming"}[status]
        out = [m for m in out if m["status"] == wanted]
    return out


def error_response(msg: str) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"success": False, "stale": False, "error": f"Upstream unavailable and no cached data yet: {msg}"},
    )


async def matches_response(category: str, status: str, details: bool) -> JSONResponse:
    touch()
    try:
        matches, fetched, stale = await get_all_matches()
    except ScrapeError as exc:
        return error_response(str(exc))
    selected = sort_matches(filter_matches(matches, category, status))
    if details:
        selected_copy = [dict(m) for m in selected]
        stale = (await enrich_live(selected_copy)) or stale
        selected = selected_copy
    return JSONResponse(content={
        "success": True,
        "stale": stale,
        "total": len(selected),
        "last_updated": iso(fetched),
        "matches": selected,
    })


# --------------------------------------------------------------------------- #
# Background refresher
# --------------------------------------------------------------------------- #
async def background_refresher() -> None:
    while True:
        try:
            await asyncio.sleep(BACKGROUND_INTERVAL)
            if time.time() - _state["last_request"] > ACTIVE_WINDOW:
                continue  # nobody is using the API: save bandwidth, stay polite upstream
            await cache.get("page:live", TTL_LIVE, lambda: load_page_matches(URL_LIVE))
            await cache.get("page:recent", TTL_OTHER, lambda: load_page_matches(URL_RECENT))
            await cache.get("page:upcoming", TTL_OTHER, lambda: load_page_matches(URL_UPCOMING))
            cache.prune()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("background refresh error: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _state["client"] = httpx.AsyncClient(
        timeout=httpx.Timeout(12.0, connect=6.0),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )
    _state["semaphore"] = asyncio.Semaphore(5)
    task = asyncio.create_task(background_refresher())
    try:
        yield
    finally:
        task.cancel()
        await _state["client"].aclose()


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
app = FastAPI(title="Live Cricket Score API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

Category = Literal["all", "international", "league", "domestic", "women"]
Status = Literal["all", "live", "recent", "upcoming"]


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "timestamp": now_iso(), "upstream_paused": bool(_state.get("upstream_blocked"))}


@app.get("/api/matches")
async def list_matches(
    category: Category = Query("all"),
    status: Status = Query("all"),
    details: bool = Query(True, description="Include batsmen/bowler/recent balls for live matches"),
):
    return await matches_response(category, status, details)


@app.get("/api/matches/live")
async def list_live(category: Category = Query("all"), details: bool = Query(True)):
    return await matches_response(category, "live", details)


@app.get("/api/matches/recent")
async def list_recent(category: Category = Query("all")):
    return await matches_response(category, "recent", False)


@app.get("/api/matches/upcoming")
async def list_upcoming(category: Category = Query("all")):
    return await matches_response(category, "upcoming", False)


@app.get("/api/match/{match_id}/scorecard")
async def scorecard(match_id: str):
    touch()
    if not match_id.isdigit():
        return JSONResponse(status_code=400, content={"success": False, "error": "match_id must be numeric"})
    try:
        data, fetched, stale = await cache.get(
            f"scorecard:{match_id}", TTL_SCORECARD, lambda: load_scorecard(match_id)
        )
    except ScrapeError as exc:
        return error_response(str(exc))
    return JSONResponse(content={"success": True, "stale": stale, "last_updated": iso(fetched), **data})


if __name__ == "__main__":
    import os

    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
