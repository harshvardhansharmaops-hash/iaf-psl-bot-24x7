#!/usr/bin/env python3
"""
IAF Agniveervayu 01/2027 PSL Monitor — GitHub Actions Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Runs as a cron job via GitHub Actions every 10 minutes.
- Checks ONLY EDCIL Agniveervayu psl.html + home page for 01/2027 PSL PDFs
- Processes Telegram commands (/status, /check, /demo, /dl, /help) + buttons
- Commits state files back to repo (needs Read & Write workflow permission)
- Uses curl_cffi + optional PROXY_URL + web proxies to bypass 403 blocks
"""

import os
import sys
import re
import json
import time
import hashlib
import html as html_lib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin, quote

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from curl_cffi import requests as cf_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ─── CONFIGURATION ────────────────────────────────────────────────────────
BOT_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PROXY_URL          = os.getenv("PROXY_URL", "").strip()  # Optional HTTP/HTTPS/SOCKS proxy secret
CHAT_IDS           = [c.strip() for c in os.getenv("CHAT_IDS", "8960222553,-1004472936994").split(",") if c.strip()]
RUN_DURATION_SEC   = int(os.getenv("RUN_DURATION_SEC", "225"))   # ~3m 45s active loop per 5-min cron run
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))  # Check EDCIL every 60 seconds

MONITOR_URLS = [
    ("EDCIL Agniveervayu PSL Page", "https://iafrecruitment.edcil.co.in/agniveervayu/psl.html"),
    ("EDCIL Agniveervayu Home",     "https://iafrecruitment.edcil.co.in/agniveervayu/"),
]

BASE_EDCIL = "https://iafrecruitment.edcil.co.in/agniveervayu/"

KEYWORDS = [
    r"provisional\s+select\s+list",
    r"PSL.*01[\s/\\-]*2027",
    r"01[\s/\\-]*2027.*PSL",
    r"select\s+list.*2027",
    r"enrol(?:l)?ment\s+list.*2027",
    r"merit\s+list.*2027",
]

IST = timezone(timedelta(hours=5, minutes=30))
MAX_TG_MSG_LEN = 3900
MAX_TG_FILE_BYTES = 48 * 1024 * 1024

# ─── STATE FILES (committed back to repo by workflow) ─────────────────────
STATE_DIR      = Path(__file__).parent / "state"
SEEN_FILE      = STATE_DIR / "seen_pdfs.txt"
UPDATE_ID_FILE = STATE_DIR / "last_update_id.txt"
HASH_FILE      = STATE_DIR / "page_hashes.json"
CHATS_FILE     = STATE_DIR / "registered_chats.txt"
STATS_FILE         = STATE_DIR / "stats.json"
DEMO_FILE          = STATE_DIR / "last_demo_pdfs.json"
WORKING_PROXY_FILE = STATE_DIR / "working_proxy.txt"

STATE_DIR.mkdir(exist_ok=True)

# ─── LOGGING ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("IAF-GitHub")

# ─── BROWSER HEADERS ─────────────────────────────────────────────────────
CHROME_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

LAST_WORKING_METHOD = None
CACHED_IN_PROXY = None
CACHED_PROXY_POOL = []

# ─── STATE HELPERS ────────────────────────────────────────────────────────
def load_seen() -> set:
    if SEEN_FILE.exists():
        return set(line.strip() for line in SEEN_FILE.read_text(encoding="utf-8").splitlines() if line.strip())
    return set()

def save_seen(key: str):
    with open(SEEN_FILE, "a", encoding="utf-8") as f:
        f.write(key + "\n")

def load_hashes() -> dict:
    if HASH_FILE.exists():
        try:
            return json.loads(HASH_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_hashes(hashes: dict):
    HASH_FILE.write_text(json.dumps(hashes, indent=2), encoding="utf-8")

def load_update_id() -> int:
    if UPDATE_ID_FILE.exists():
        try:
            return int(UPDATE_ID_FILE.read_text().strip())
        except Exception:
            pass
    return 0

def save_update_id(uid: int):
    UPDATE_ID_FILE.write_text(str(uid), encoding="utf-8")

def load_registered_chats() -> set:
    chats = set(CHAT_IDS)
    if CHATS_FILE.exists():
        for line in CHATS_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip():
                chats.add(line.strip())
    return chats

def save_registered_chat(chat_id: str, all_chats: set):
    if chat_id not in all_chats:
        all_chats.add(chat_id)
        with open(CHATS_FILE, "a", encoding="utf-8") as f:
            f.write(chat_id + "\n")

def load_stats() -> dict:
    default = {
        "total_checks": 0,
        "total_runs": 0,
        "pdfs_found": 0,
        "last_check": "Never",
        "last_method": "Not run yet",
        "last_page_statuses": {},
        "heartbeat": True,
    }
    if STATS_FILE.exists():
        try:
            data = json.loads(STATS_FILE.read_text(encoding="utf-8"))
            default.update(data)
            return default
        except Exception:
            pass
    return default

def save_stats(stats: dict):
    STATS_FILE.write_text(json.dumps(stats, indent=2), encoding="utf-8")

def load_demo_pdfs() -> list:
    if DEMO_FILE.exists():
        try:
            return json.loads(DEMO_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []

def save_demo_pdfs(items: list):
    DEMO_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")

# ─── FETCH ENGINE (Direct + Auto Indian Proxy Bypass) ─────────────────────
def _proxies_dict(proxy_str: str = ""):
    p = (proxy_str or PROXY_URL).strip()
    if p:
        return {"http": p, "https": p}
    return None

def _save_working_proxy(proxy_str: str):
    global CACHED_IN_PROXY
    CACHED_IN_PROXY = proxy_str
    try:
        WORKING_PROXY_FILE.write_text(proxy_str, encoding="utf-8")
    except Exception:
        pass

def _load_saved_proxy() -> str:
    global CACHED_IN_PROXY
    if CACHED_IN_PROXY:
        return CACHED_IN_PROXY
    if WORKING_PROXY_FILE.exists():
        p = WORKING_PROXY_FILE.read_text(encoding="utf-8").strip()
        if p:
            CACHED_IN_PROXY = p
            return p
    return ""

def load_indian_proxies() -> list:
    """Fetches fresh free HTTPS/SOCKS5 proxies (prioritizing India, then global HTTPS/SOCKS5)."""
    global CACHED_PROXY_POOL
    if CACHED_PROXY_POOL:
        return CACHED_PROXY_POOL

    found = []
    seen_p = set()

    def _add(p: str):
        p = p.strip()
        if not p or ":" not in p:
            return
        if "://" not in p:
            p = f"http://{p}"
        if p.startswith("socks5://"):
            p = "socks5h://" + p[len("socks5://"):]
        # Note: many proxy lists write https://ip:port for HTTP CONNECT proxies
        variants = [p]
        if p.startswith("https://"):
            variants.insert(0, "http://" + p[len("https://"):])
        for v in variants:
            if v not in seen_p:
                seen_p.add(v)
                found.append(v)

    # 1. Proxifly India feed (all protocols)
    for url in [
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/countries/IN/data.txt",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/countries/SG/data.txt",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/countries/AE/data.txt",
    ]:
        try:
            r = requests.get(url, timeout=6)
            if r.status_code == 200 and len(r.text) > 10:
                for line in r.text.splitlines():
                    _add(line)
        except Exception:
            pass

    # 2. ProxyScrape v4 (India HTTP & SOCKS5)
    for proto in ("socks5", "http"):
        try:
            u = f"https://api.proxyscrape.com/v4/free-proxy-list/get?request=displayproxies&protocol={proto}&country=in&timeout=5000"
            r = requests.get(u, timeout=6)
            if r.status_code == 200 and len(r.text) > 10:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and ":" in line:
                        _add(f"{proto}://{line}")
        except Exception:
            pass

    # 3. Proxifly pre-verified HTTPS & SOCKS5 global feeds (often support CONNECT tunneling cleanly)
    for url in [
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
    ]:
        try:
            r = requests.get(url, timeout=6)
            if r.status_code == 200 and len(r.text) > 10:
                lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
                for line in lines[:35]:
                    _add(line)
        except Exception:
            pass

    # Prioritize India / SG / AE & socks5h / http CONNECT proxies
    found.sort(key=lambda x: 0 if x.startswith("socks5h://") else (1 if x.startswith("http://") else 2))
    CACHED_PROXY_POOL = found[:80]
    log.info("  🌐 Loaded %d proxy candidates (sample: %s)", len(CACHED_PROXY_POOL), CACHED_PROXY_POOL[:3])
    return CACHED_PROXY_POOL

def _fetch_via_proxy(proxy_str: str, url: str, timeout: int = 7):
    proxies = _proxies_dict(proxy_str)
    try:
        if HAS_CURL_CFFI:
            resp = cf_requests.get(
                url,
                impersonate="chrome",
                proxies=proxies,
                timeout=timeout,
                verify=False,
                headers={"Accept-Language": "en-US,en;q=0.9,hi;q=0.8"},
            )
        else:
            resp = requests.get(
                url,
                headers=CHROME_HEADERS,
                proxies=proxies,
                timeout=timeout,
                verify=False,
                allow_redirects=True,
            )
        body_low = resp.text.lower()
        if resp.status_code == 200 and len(resp.text) > 400 and "<html" in body_low and "403 forbidden" not in body_low[:400]:
            return proxy_str, resp.text, 200
        return None, None, f"HTTP_{resp.status_code}"
    except Exception as e:
        return None, None, f"ERR:{str(e)[:40]}"

def _fetch_via_jina(url: str) -> tuple:
    """Try Jina Reader API with X-Respond-With: html (renders page in headless browser)."""
    try:
        jina_url = f"https://r.jina.ai/{url}"
        resp = requests.get(
            jina_url,
            headers={
                "X-Respond-With": "html",
                "User-Agent": CHROME_HEADERS["User-Agent"],
                "Accept": "text/html,*/*;q=0.8",
            },
            timeout=18,
        )
        body_low = resp.text.lower()
        if (
            resp.status_code == 200
            and len(resp.text) > 500
            and "<html" in body_low
            and "403 forbidden" not in body_low[:500]
            and ("agniveer" in body_low or "edcil" in body_low or "psl" in body_low)
        ):
            log.info("  ✅ [Jina Reader] HTTP 200 HTML fetched for %s (%d bytes)", url.split("/")[-1] or "home", len(resp.text))
            return 200, resp.text
        log.info("  [Jina Reader] HTTP %d (%d bytes) for %s", resp.status_code, len(resp.text), url.split("/")[-1] or "home")
    except Exception as e:
        log.warning("  [Jina Reader] Error: %s", str(e)[:90])
    return None, None

def fetch_page(url: str) -> tuple:
    """
    Tries multiple methods to fetch HTML:
    0. Custom PROXY_URL secret (if configured)
    1. Direct curl_cffi Chrome TLS (fast check; works if IP isn't geo-blocked)
    2. Cached working Indian proxy (instant)
    3. Parallel batch scan across live Indian proxies (Proxifly / ProxyScrape / Geonode)
    Returns (status_code_or_none, html_or_none, method_name).
    """
    global LAST_WORKING_METHOD, CACHED_IN_PROXY, CACHED_PROXY_POOL
    last_http_status = None
    proxies = _proxies_dict()

    # Method 0: Explicit custom proxy from PROXY_URL secret
    if proxies:
        try:
            resp = (
                cf_requests.get(url, impersonate="chrome", proxies=proxies, timeout=15, verify=False)
                if HAS_CURL_CFFI else
                requests.get(url, headers=CHROME_HEADERS, proxies=proxies, timeout=15, verify=False)
            )
            last_http_status = resp.status_code
            if resp.status_code == 200 and len(resp.text) > 100:
                LAST_WORKING_METHOD = "proxy_env"
                log.info("  ✅ [PROXY_URL] HTTP 200 for %s", url)
                return 200, resp.text, "Custom Proxy"
            log.info("  [PROXY_URL] HTTP %d for %s", resp.status_code, url)
        except Exception as e:
            log.warning("  [PROXY_URL] Error: %s", str(e)[:90])

    # Method 1: Fast direct check (skip repeat direct tries once known 403)
    if LAST_WORKING_METHOD in (None, "curl_cffi"):
        try:
            resp = (
                cf_requests.get(url, impersonate="chrome", timeout=10, headers={"Accept-Language": "en-US,en;q=0.9,hi;q=0.8"})
                if HAS_CURL_CFFI else
                requests.get(url, headers=CHROME_HEADERS, timeout=10, verify=False, allow_redirects=True)
            )
            last_http_status = resp.status_code
            if resp.status_code == 200 and len(resp.text) > 200:
                LAST_WORKING_METHOD = "curl_cffi"
                log.info("  ✅ [Direct] HTTP 200 for %s", url)
                return 200, resp.text, "Direct"
            log.info("  [Direct] HTTP %d for %s -> switching to Indian Proxy pool", resp.status_code, url.split("/")[-1] or "home")
        except Exception as e:
            log.warning("  [Direct] Error: %s", str(e)[:80])

    # Method 2: Try previously cached proxy first
    saved_p = _load_saved_proxy()
    if saved_p:
        ok_p, ok_html, _ = _fetch_via_proxy(saved_p, url, timeout=8)
        if ok_html:
            LAST_WORKING_METHOD = "in_proxy"
            log.info("  ✅ [Proxy Cached] HTTP 200 via %s", ok_p.split("@")[-1])
            return 200, ok_html, "Indian Proxy"
        CACHED_IN_PROXY = None

    # Method 3: Try Google Translate HTML proxy fallback (free, non-Azure/AWS edge)
    try:
        tr_url = url.replace("https://iafrecruitment.edcil.co.in/", "https://iafrecruitment-edcil-co-in.translate.goog/")
        sep = "&" if "?" in tr_url else "?"
        tr_url = f"{tr_url}{sep}_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en"
        r_tr = requests.get(tr_url, headers=CHROME_HEADERS, timeout=10, verify=False, allow_redirects=True)
        if r_tr.status_code == 200 and len(r_tr.text) > 500 and "403 forbidden" not in r_tr.text.lower()[:400]:
            log.info("  ✅ [Translate Proxy] HTTP 200 for %s", url.split("/")[-1] or "home")
            return 200, r_tr.text, "Translate Proxy"
        log.info("  [Translate Proxy] HTTP %d (%d bytes)", r_tr.status_code, len(r_tr.text))
    except Exception as e:
        log.info("  [Translate Proxy] Error: %s", str(e)[:60])

    # Method 4: Parallel scan across HTTPS/SOCKS5/IN proxies
    candidates = load_indian_proxies()
    if not candidates:
        CACHED_PROXY_POOL = []
        candidates = load_indian_proxies()

    batch_size = 20
    seen_codes = {}
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i + batch_size]
        pool = ThreadPoolExecutor(max_workers=len(batch))
        try:
            futures = [pool.submit(_fetch_via_proxy, p, url, 7) for p in batch]
            for fut in as_completed(futures):
                ok_p, ok_html, p_code = fut.result()
                if p_code:
                    seen_codes[p_code] = seen_codes.get(p_code, 0) + 1
                if ok_html:
                    _save_working_proxy(ok_p)
                    LAST_WORKING_METHOD = "in_proxy"
                    log.info("  ✅ [Proxy Found] HTTP 200 via %s", ok_p)
                    return 200, ok_html, "Proxy Tunnel"
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    if seen_codes:
        log.info("  📊 Proxy response summary: %s", seen_codes)

    # Reset proxy pool for next run if none worked this batch
    CACHED_PROXY_POOL = []
    return last_http_status or 403, None, "Blocked/Failed"

def probe_pdf_exists(url: str) -> bool:
    """Fast HEAD/Range probe using Direct or Custom Proxy only."""
    proxies = _proxies_dict()
    try:
        if LAST_WORKING_METHOD == "proxy_env" and proxies:
            r = requests.head(url, headers=CHROME_HEADERS, proxies=proxies, timeout=6, verify=False, allow_redirects=True)
            return r.status_code == 200
        if LAST_WORKING_METHOD == "curl_cffi" and HAS_CURL_CFFI:
            r = cf_requests.get(url, impersonate="chrome", timeout=6, headers={"Range": "bytes=0-15"})
            return r.status_code in (200, 206) and r.content.startswith(b"%PDF")
    except Exception:
        pass
    return False

def _download_pdf_proxy(proxy_str: str, url: str) -> bytes | None:
    proxies = _proxies_dict(proxy_str)
    try:
        resp = (
            cf_requests.get(url, impersonate="chrome", proxies=proxies, timeout=25, verify=False)
            if HAS_CURL_CFFI else
            requests.get(url, headers=CHROME_HEADERS, proxies=proxies, timeout=25, verify=False)
        )
        if resp.status_code == 200 and resp.content.startswith(b"%PDF"):
            return resp.content
    except Exception:
        pass
    return None

def fetch_pdf(url: str) -> bytes | None:
    """Download full PDF bytes using Custom Proxy, Direct, or Working Indian Proxies."""
    if PROXY_URL:
        data = _download_pdf_proxy(PROXY_URL, url)
        if data:
            return data

    # Direct attempt
    try:
        resp = (
            cf_requests.get(url, impersonate="chrome", timeout=15)
            if HAS_CURL_CFFI else
            requests.get(url, headers=CHROME_HEADERS, timeout=15, verify=False)
        )
        if resp.status_code == 200 and resp.content.startswith(b"%PDF"):
            return resp.content
    except Exception:
        pass

    # Cached Indian proxy
    saved_p = _load_saved_proxy()
    if saved_p:
        data = _download_pdf_proxy(saved_p, url)
        if data:
            return data

    # Fallback through top Indian proxy candidates
    for p in load_indian_proxies()[:15]:
        data = _download_pdf_proxy(p, url)
        if data:
            _save_working_proxy(p)
            return data

    return None

# ─── TELEGRAM API ─────────────────────────────────────────────────────────
def send_msg(chat_id: str, text: str, reply_markup: dict = None):
    if len(text) > MAX_TG_MSG_LEN:
        text = text[:MAX_TG_MSG_LEN]
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=payload,
            timeout=15,
        )
        if not r.ok:
            log.warning("Telegram API error for %s (%d): %s", chat_id, r.status_code, r.text[:200])
            if reply_markup:
                payload.pop("reply_markup", None)
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    data=payload,
                    timeout=15,
                )
    except Exception as e:
        log.error("Send msg failed for %s: %s", chat_id, e)

def broadcast(text: str, chats: set):
    for cid in list(chats):
        send_msg(cid, text)

def send_pdf(chat_id: str, pdf_bytes: bytes, filename: str, caption: str) -> bool:
    if len(pdf_bytes) > MAX_TG_FILE_BYTES:
        send_msg(chat_id, f"⚠️ <code>{html_lib.escape(filename)}</code> 50 MB se badi hai — direct link se download karein.")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={"chat_id": chat_id, "caption": caption[:1000], "parse_mode": "HTML"},
            files={"document": (filename, pdf_bytes, "application/pdf")},
            timeout=60,
        )
        return resp.ok
    except Exception as e:
        log.error("Send PDF failed for %s: %s", chat_id, e)
        return False

def broadcast_pdf(pdf_bytes: bytes, filename: str, caption: str, chats: set):
    for cid in list(chats):
        send_pdf(cid, pdf_bytes, filename, caption)
        time.sleep(0.3)

def edit_msg(chat_id: str, message_id: int, text: str, reply_markup: dict = None):
    """Edits an existing Telegram message in-place (used for Back / Next pagination)."""
    if len(text) > MAX_TG_MSG_LEN:
        text = text[:MAX_TG_MSG_LEN]
    try:
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            data=payload,
            timeout=15,
        )
        if not r.ok and "message is not modified" not in r.text.lower():
            log.warning("Telegram editMessage error (%d): %s", r.status_code, r.text[:200])
    except Exception as e:
        log.error("Edit msg failed for %s: %s", chat_id, e)

def answer_callback(cb_id: str, text: str = ""):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
            data={"callback_query_id": cb_id, "text": text[:190]},
            timeout=10,
        )
    except Exception:
        pass

# ─── PDF FILTER (Only PSL for auto-alerts) ────────────────────────────────
def is_psl_pdf(name: str) -> bool:
    clean = name.lower()
    blacklist = [
        "guideline", "brochure", "application_form", "syllabus", "model",
        "normalisation", "exam", "instructions", "omr", "tutorial", "mock",
        "diploma", "checklist", "nc_", "non-combatant",
    ]
    if any(b in clean for b in blacklist):
        return False
    whitelist = [
        "psl", "provisional", "enrolment", "enrol", "ss_men", "ss_women",
        "os_men", "os_women", "science_subjects", "other_than_science", "additional_psl",
    ]
    return any(w in clean for w in whitelist)

# ─── EXTRACT ALL PDF LINKS (FRONT-END + CODE/COMMENTS) ────────────────────
def extract_all_pdfs_detailed(html: str, page_url: str, page_label: str) -> list:
    """
    Separates visible Front-End PDFs from Hidden/Commented HTML Code PDFs.
    Returns list of dicts: {url, filename, page, location}
    """
    found = {}  # full_url -> dict

    def _add(raw_link: str, location: str):
        raw_link = raw_link.strip().replace("\\", "/")
        if not raw_link or raw_link.startswith(("javascript:", "mailto:", "#")):
            return
        full = urljoin(page_url, raw_link).replace(" ", "%20")
        if not full.startswith(("http://", "https://")):
            return
        if full not in found:
            fname = full.split("/")[-1].split("?")[0] or "document.pdf"
            found[full] = {
                "url": full,
                "filename": fname,
                "page": page_label,
                "location": location,
            }

    # 1. Visible Front-End HTML (strip comments first)
    visible_html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    for m in re.findall(r'(?:href|src|data-href|data-url)\s*=\s*["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']', visible_html, re.IGNORECASE):
        _add(m, "Front-End")
    for m in re.findall(r'["\']([^"\'\s<>]+\.pdf(?:\?[^"\'\s<>]*)?)["\']', visible_html, re.IGNORECASE):
        _add(m, "Front-End/JS")
    for m in re.findall(r'(https?://[^\s"\'<>]+\.pdf(?:\?[^\s"\'<>]*)?)', visible_html, re.IGNORECASE):
        _add(m, "Front-End")

    # 2. Hidden / Commented HTML Code <!-- ... -->
    for comment in re.findall(r'<!--(.*?)-->', html, re.DOTALL):
        for m in re.findall(r'(?:href|src|data-href|data-url)\s*=\s*["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']', comment, re.IGNORECASE):
            _add(m, "Code Comment")
        for m in re.findall(r'["\']([^"\'\s<>]+\.pdf(?:\?[^"\'\s<>]*)?)["\']', comment, re.IGNORECASE):
            _add(m, "Code Comment")
        for m in re.findall(r'(https?://[^\s"\'<>]+\.pdf(?:\?[^\s"\'<>]*)?)', comment, re.IGNORECASE):
            _add(m, "Code Comment")

    return list(found.values())

# ─── PREDICTED PSL PDF URLS ──────────────────────────────────────────────
def generate_predicted_urls() -> list:
    today = datetime.now(IST)
    dates = [today.strftime("%d%b%Y"), today.strftime("%b%Y")]

    psl_names = [
        "AGNIVEERVAYU_(MEN)_(SCIENCE_SUBJECTS).pdf",
        "AGNIVEERVAYU_(WOMEN)_(SCIENCE_SUBJECTS).pdf",
        "AGNIVEERVAYU_(MEN)_(OTHER_THAN_SCIENCE_SUBJECTS).pdf",
        "AGNIVEERVAYU_(WOMEN)_(OTHER_THAN_SCIENCE_SUBJECTS).pdf",
    ]
    enrol_names = [
        "SS_MEN.pdf", "SS_MEN_.pdf", "SS_WOMEN.pdf", "SS_WOMEN_.pdf",
        "OS_MEN.pdf", "OS_MEN_.pdf", "OS_WOMEN.pdf", "OS_WOMEN_.pdf",
    ]

    urls = []
    for d in dates:
        for n in enrol_names:
            urls.append(f"{BASE_EDCIL}img/psl/{d}/ENROLMENT_LIST/{n}")
            urls.append(f"{BASE_EDCIL}img/psl/{d}/PSL/{n}")
        for n in psl_names:
            urls.append(f"{BASE_EDCIL}img/psl/{d}/PSL/{n}")

    return list(dict.fromkeys(urls))

# ─── CHECK CYCLE ──────────────────────────────────────────────────────────
def run_check(chats: set, stats: dict, send_heartbeat: bool = False) -> str:
    now_str = datetime.now(IST).strftime("%H:%M:%S")
    seen = load_seen()
    hashes = load_hashes()
    page_statuses = {}
    new_pdfs = []
    changed_pages = []
    used_method = "Unknown"
    any_direct_ok = False

    log.info("═══ Check Cycle at %s (IST) ═══", now_str)

    # 1. Check official EDCIL pages
    for page_name, page_url in MONITOR_URLS:
        status, html, method = fetch_page(page_url)
        used_method = method

        if not html:
            page_statuses[page_name] = f"HTTP {status} (Blocked)" if status else "Timeout/Error"
            continue

        page_statuses[page_name] = f"HTTP {status} ✅ ({method})"
        if method in ("Direct", "Custom Proxy"):
            any_direct_ok = True

        # Check for page content changes
        new_hash = hashlib.md5(html.encode("utf-8", errors="ignore")).hexdigest()
        old_hash = hashes.get(page_name, "")
        if old_hash and new_hash != old_hash:
            log.info("  🔔 [%s] Page content CHANGED!", page_name)
            changed_pages.append(page_name)
            for kw in KEYWORDS:
                matches = re.findall(kw, html, re.IGNORECASE)
                if matches:
                    key = hashlib.md5(f"{page_name}|{kw}|{matches[0]}".encode()).hexdigest()
                    if key not in seen:
                        seen.add(key)
                        save_seen(key)
                        alert = (
                            f"🚨 <b>ALERT: PSL Related Change Detected!</b>\n\n"
                            f"📍 Page: <b>{page_name}</b>\n"
                            f"🔑 Keyword: <code>{html_lib.escape(matches[0])}</code>\n"
                            f"🔗 {page_url}\n\n"
                            f"⚡ <i>Checking for downloadable PDFs...</i>"
                        )
                        broadcast(alert, chats)
        hashes[page_name] = new_hash

        # Extract all PDF links & auto-send PSL PDFs
        for item in extract_all_pdfs_detailed(html, page_url, page_name):
            pdf_url = item["url"]
            if pdf_url not in seen and is_psl_pdf(pdf_url):
                log.info("  🎯 NEW PSL PDF in HTML (%s): %s", item["location"], pdf_url)
                pdf_data = fetch_pdf(pdf_url)
                if pdf_data and len(pdf_data) > 5000:
                    fname = re.sub(r'[^\w.\-]', '_', item["filename"])
                    seen.add(pdf_url)
                    save_seen(pdf_url)
                    new_pdfs.append(fname)
                    stats["pdfs_found"] += 1

                    caption = (
                        f"🚨 <b>IAF AGNIVEERVAYU 01/2027 — PSL OUT!</b>\n\n"
                        f"📄 <b>{html_lib.escape(fname)}</b>\n"
                        f"📦 Size: {len(pdf_data)/1024:.1f} KB\n"
                        f"📍 Source: {item['page']} ({item['location']})\n"
                        f"🔗 <code>{html_lib.escape(pdf_url)}</code>"
                    )
                    broadcast_pdf(pdf_data, fname, caption, chats)

    save_hashes(hashes)

    # 2. Fast probe predicted URLs ONLY if direct/custom proxy access succeeded
    predicted = generate_predicted_urls()
    probed_count = 0
    if any_direct_ok:
        for purl in predicted:
            if purl in seen:
                continue
            probed_count += 1
            if probe_pdf_exists(purl):
                pdf_data = fetch_pdf(purl)
                if pdf_data and len(pdf_data) > 5000:
                    fname = re.sub(r'[^\w.\-]', '_', purl.split("/")[-1])
                    if is_psl_pdf(fname):
                        seen.add(purl)
                        save_seen(purl)
                        new_pdfs.append(fname)
                        stats["pdfs_found"] += 1
                        caption = (
                            f"🚨 <b>IAF AGNIVEERVAYU 01/2027 — PSL OUT!</b>\n\n"
                            f"📄 <b>{html_lib.escape(fname)}</b>\n"
                            f"📦 Size: {len(pdf_data)/1024:.1f} KB\n"
                            f"🔗 <code>{html_lib.escape(purl)}</code>"
                        )
                        broadcast_pdf(pdf_data, fname, caption, chats)

    # Update stats
    stats["total_checks"] += 1
    stats["last_check"] = datetime.now(IST).strftime("%d %b %Y, %H:%M:%S IST")
    stats["last_method"] = used_method
    stats["last_page_statuses"] = page_statuses

    status_lines = "\n".join(f"  • {k}: <code>{v}</code>" for k, v in page_statuses.items())
    change_str = (
        f"🚨 <b>BADLAV DETECT HUA!</b> ({', '.join(changed_pages)})"
        if changed_pages else
        "❌ Koi badlav nahi (No change)"
    )
    summary = (
        f"✅ <b>Check #{stats['total_checks']} at {now_str} IST</b>\n\n"
        f"{status_lines}\n"
        f"• Web Change: {change_str}\n"
        f"• Direct URL Probes: <b>{probed_count}</b>\n"
        f"• New PSL PDFs: <b>{len(new_pdfs)}</b>\n\n"
    )
    if new_pdfs:
        summary += f"🎯 <b>FOUND:</b> {', '.join(new_pdfs)}\n"
    elif not any(s.startswith("HTTP 200") for s in page_statuses.values()):
        summary += (
            f"⚠️ <i>GitHub US IP ko EDCIL ne block (403) kiya aur fallback web proxy fail hui.\n"
            f"Bypass karne ke liye GitHub Secret me <code>PROXY_URL</code> (Indian proxy) add karein.</i>"
        )
    else:
        summary += f"<i>PSL 01/2027 abhi release nahi hui. Tracking active hai!</i>"

    if send_heartbeat and stats.get("heartbeat", True):
        hb_msg = (
            f"🕒 <b>5-Min Tracker Update ({now_str} IST)</b>\n"
            f"{status_lines}\n"
            f"• Web Change: {change_str}\n"
            f"• Next 4 min har {CHECK_INTERVAL_SEC}s par auto-check ON\n"
            f"<i>(Band karne ke liye: <code>/heartbeat off</code>)</i>"
        )
        broadcast(hb_msg, chats)

    return summary

# ─── DEMO: SCAN ALL PDFs (FRONT-END + CODE) & PROVIDE DOWNLOAD OPTIONS ────
def run_demo(chat_id: str):
    """
    Scans both PSL Page + Home Page (visible front-end AND hidden/commented HTML),
    lists EVERY PDF found, gives 1-tap direct download buttons AND /dl options.
    """
    log.info("🧪 [DEMO] Full PDF scan for chat %s", chat_id)

    all_items = []
    seen_urls = set()
    page_errors = []

    for page_label, page_url in MONITOR_URLS:
        status, html, method = fetch_page(page_url)
        if not html:
            page_errors.append(f"⚠️ {page_label}: HTTP {status or 'Blocked/Error'}")
            continue

        for item in extract_all_pdfs_detailed(html, page_url, page_label):
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                all_items.append(item)

    if not all_items:
        err_text = "\n".join(page_errors) + "\n\n" if page_errors else ""
        send_msg(
            chat_id,
            f"🧪 <b>DEMO Scan Complete</b>\n\n"
            f"{err_text}"
            f"❌ Website ke front-end ya HTML code me koi bhi PDF link nahi mili.\n"
            f"<i>Agar 403 error dikh raha hai, toh GitHub Secret me Indian <code>PROXY_URL</code> lagane ki zaroorat hogi.</i>"
        )
        return

    # Save scanned PDFs so /dl <num> or callback buttons work anytime
    save_demo_pdfs(all_items)

    msg, reply_markup = build_demo_page_payload(all_items, 0)
    send_msg(chat_id, msg, reply_markup=reply_markup)

def build_demo_page_payload(all_items: list, page_idx: int) -> tuple:
    """Builds a single paginated Telegram message (8 PDFs/page) with Back & Next buttons."""
    per_page = 8
    total = len(all_items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_idx = max(0, min(page_idx, total_pages - 1))

    start = page_idx * per_page
    chunk = all_items[start:start + per_page]

    lines = []
    inline_rows = []
    for offset, item in enumerate(chunk):
        idx = start + offset + 1
        safe_name = html_lib.escape(item["filename"])
        safe_url = html_lib.escape(item["url"], quote=True)
        tag = "🆕 PSL" if is_psl_pdf(item["filename"]) else item["location"]
        lines.append(
            f"{idx}. 📄 <b>{safe_name}</b>\n"
            f"    📍 {item['page']} • <i>[{tag}]</i>\n"
            f"    🔗 <a href=\"{safe_url}\">Direct 1-Tap Download</a>"
        )
        short_lbl = item["filename"][:22]
        inline_rows.append([
            {"text": f"🌐 #{idx} {short_lbl}", "url": item["url"]},
            {"text": f"🤖 File #{idx}", "callback_data": f"dl:{idx}"},
        ])

    if total_pages > 1:
        nav_row = []
        if page_idx > 0:
            nav_row.append({"text": "⬅️ Back", "callback_data": f"page:{page_idx - 1}"})
        nav_row.append({"text": f"📄 Page {page_idx + 1}/{total_pages}", "callback_data": "noop"})
        if page_idx < total_pages - 1:
            nav_row.append({"text": "Next ➡️", "callback_data": f"page:{page_idx + 1}"})
        inline_rows.append(nav_row)

    msg = (
        f"🧪 <b>DEMO: Website & Code Scan ({total} PDFs Found)</b>\n"
        f"📄 <b>Page {page_idx + 1} / {total_pages}</b> (Showing #{start + 1}–#{start + len(chunk)})\n\n"
        + "\n\n".join(lines)
        + "\n\n━━━━━━━━━━━━━━━\n"
        + "📥 <b>1-Second Download:</b> Button <b>🌐</b> dabakar browser me turant download karein, ya <b>🤖 File</b> dabakar Telegram me mangwayein (ya <code>/dl 1</code>)."
    )
    return msg, {"inline_keyboard": inline_rows}

def handle_dl_command(chat_id: str, arg: str):
    """Download and send requested PDF(s) from the latest /demo scan."""
    items = load_demo_pdfs()
    if not items:
        send_msg(chat_id, "ℹ️ Pehle <code>/demo</code> bhejiye taaki website ki saari PDFs scan ho sakein.")
        return

    targets = []
    arg = arg.strip().lower()
    if arg == "all":
        targets = items
    elif arg.isdigit():
        idx = int(arg) - 1
        if 0 <= idx < len(items):
            targets = [items[idx]]
        else:
            send_msg(chat_id, f"❌ Invalid number! <code>1</code> se <code>{len(items)}</code> ke beech number bhejein.")
            return
    else:
        send_msg(chat_id, "💡 Use karein: <code>/dl 1</code> (ek PDF ke liye) ya <code>/dl all</code> (saari PDFs ke liye).")
        return

    send_msg(chat_id, f"⬇️ <i>{len(targets)} PDF download karke Telegram me bhej raha hoon...</i>")
    sent = 0
    failed_links = []

    for item in targets:
        fname = re.sub(r'[^\w.\-]', '_', item["filename"])
        pdf_bytes = fetch_pdf(item["url"])
        if pdf_bytes and len(pdf_bytes) > 500:
            caption = (
                f"📄 <b>{html_lib.escape(fname)}</b>\n"
                f"📦 Size: {len(pdf_bytes)/1024:.1f} KB\n"
                f"📍 {item['page']} ({item['location']})"
            )
            if send_pdf(chat_id, pdf_bytes, fname, caption):
                sent += 1
                time.sleep(0.4)
            else:
                failed_links.append(item)
        else:
            failed_links.append(item)

    if failed_links:
        fb_lines = "\n".join(
            f"• <a href=\"{html_lib.escape(x['url'], quote=True)}\">{html_lib.escape(x['filename'])}</a>"
            for x in failed_links
        )
        send_msg(
            chat_id,
            f"⚠️ {len(failed_links)} PDF server block (403) ke karan bot se download nahi hui.\n"
            f"Apne phone browser se direct download karein:\n{fb_lines}"
        )
    elif sent:
        send_msg(chat_id, f"✅ Done! <b>{sent}</b> PDF Telegram me bhej di gayi.")

# ─── TELEGRAM COMMAND & BUTTON PROCESSOR ─────────────────────────────────
def process_telegram_commands(chats: set, stats: dict, poll_timeout: int = 4) -> bool:
    """
    Processes pending Telegram messages and inline button clicks using long-polling.
    Returns True if run_check() was already executed by /check.
    """
    last_id = load_update_id()
    checked_in_cmd = False

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params={
                "offset": last_id + 1,
                "timeout": poll_timeout,
                "allowed_updates": '["message","callback_query"]',
            },
            timeout=poll_timeout + 8,
        )
        data = resp.json()
        if not data.get("ok"):
            return False

        updates = data.get("result", [])
        if not updates:
            return False

        log.info("📨 Processing %d Telegram updates...", len(updates))

        for update in updates:
            uid = update["update_id"]
            if uid > last_id:
                last_id = uid

            # Handle inline button click (page:N, noop, dl:N)
            cb = update.get("callback_query")
            if cb:
                cb_id = cb.get("id", "")
                cb_data = cb.get("data", "")
                cb_msg = cb.get("message", {})
                cb_chat = str(cb_msg.get("chat", {}).get("id", ""))
                cb_msg_id = cb_msg.get("message_id")
                if cb_data.startswith("page:") and cb_chat and cb_msg_id:
                    items = load_demo_pdfs()
                    if not items:
                        answer_callback(cb_id, "List expire ho gayi, /demo dubara bhejein.")
                    else:
                        try:
                            page_num = int(cb_data.split(":", 1)[1])
                        except ValueError:
                            page_num = 0
                        answer_callback(cb_id, f"Page {page_num + 1}")
                        p_text, p_markup = build_demo_page_payload(items, page_num)
                        edit_msg(cb_chat, cb_msg_id, p_text, reply_markup=p_markup)
                elif cb_data == "noop":
                    answer_callback(cb_id, "Back / Next dabayein")
                elif cb_data.startswith("dl:") and cb_chat:
                    answer_callback(cb_id, "PDF fetch kar raha hoon...")
                    handle_dl_command(cb_chat, cb_data.split(":", 1)[1])
                continue

            msg = update.get("message", {})
            text = msg.get("text", "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            if not chat_id or not text:
                continue

            save_registered_chat(chat_id, chats)
            parts = text.split(maxsplit=1)
            cmd = parts[0].lower().split("@")[0].strip()
            arg = parts[1] if len(parts) > 1 else ""

            if cmd in ("/start", "/help"):
                send_msg(
                    chat_id,
                    "🛩️ <b>IAF Agniveervayu 01/2027 PSL Monitor</b>\n\n"
                    "Ye bot GitHub Actions par 24×7 chal raha hai — bina laptop ke!\n"
                    f"Har 5 minute schedule ke andar har {CHECK_INTERVAL_SEC} second me EDCIL check hoti hai.\n\n"
                    "<b>Commands:</b>\n"
                    "• <code>/status</code> - Live status aur last check details\n"
                    "• <code>/check</code> - Site check report dekhein\n"
                    "• <code>/demo</code> - Website + Code se saari PDFs (Back/Next + Download)\n"
                    "• <code>/dl 1</code> ya <code>/dl all</code> - Scanned PDFs Telegram me mangwayein\n"
                    "• <code>/heartbeat on</code> / <code>off</code> - Har 5-min status update ON/OFF\n"
                    "• <code>/help</code> - Ye menu\n\n"
                    "⚡ <i>PSL release hote hi PDF auto-deliver ho jayegi!</i>"
                )

            elif cmd == "/status":
                now_ist = datetime.now(IST).strftime("%H:%M:%S IST")
                last_statuses = stats.get("last_page_statuses", {})
                if last_statuses:
                    status_text = "\n".join(f"  • {k}: <code>{v}</code>" for k, v in last_statuses.items())
                else:
                    status_text = "\n".join(f"  • {name}: <i>Pending first run</i>" for name, _ in MONITOR_URLS)
                hb_state = "🟢 ON" if stats.get("heartbeat", True) else "🔴 OFF"

                send_msg(
                    chat_id,
                    f"📊 <b>IAF PSL GitHub Bot Status</b>\n\n"
                    f"• <b>Bot:</b> 🟢 ACTIVE (5-Min Cron + {CHECK_INTERVAL_SEC}s Loop)\n"
                    f"• <b>Check Interval:</b> Har {CHECK_INTERVAL_SEC} second (~4 min window)\n"
                    f"• <b>5-Min Heartbeat Msg:</b> {hb_state}\n"
                    f"• <b>Total Checks:</b> {stats['total_checks']}\n"
                    f"• <b>Last Check:</b> {stats['last_check']}\n"
                    f"• <b>Fetch Method:</b> <code>{stats.get('last_method', 'N/A')}</code>\n"
                    f"• <b>PSL PDFs Found:</b> {stats['pdfs_found']}\n"
                    f"• <b>Active Chats:</b> {len(chats)}\n"
                    f"• <b>Run #{stats['total_runs']}</b> at {now_ist}\n\n"
                    f"<b>Monitored Pages:</b>\n{status_text}\n\n"
                    f"🎯 <b>PSL 01/2027:</b> Tracking Active!"
                )

            elif cmd == "/check":
                send_msg(chat_id, "🔍 <i>EDCIL check kar raha hoon...</i>")
                result = run_check(chats, stats, send_heartbeat=False)
                send_msg(chat_id, result)
                checked_in_cmd = True

            elif cmd == "/demo":
                send_msg(chat_id, "🧪 <i>PSL Page + Home Page (Front-End + Code) scan kar raha hoon...</i>")
                run_demo(chat_id)

            elif cmd == "/dl":
                handle_dl_command(chat_id, arg)

            elif cmd == "/heartbeat":
                mode = arg.strip().lower()
                if mode == "off":
                    stats["heartbeat"] = False
                    send_msg(chat_id, "🔕 Har 5-minute ka heartbeat status message <b>OFF</b> kar diya gaya hai. <i>(PSL change/PDF alerts fir bhi turant aate rahenge!)</i>")
                elif mode == "on":
                    stats["heartbeat"] = True
                    send_msg(chat_id, "🔔 Har 5-minute ka heartbeat status message <b>ON</b> kar diya gaya hai!")
                else:
                    cur = "ON" if stats.get("heartbeat", True) else "OFF"
                    send_msg(chat_id, f"ℹ️ 5-Min Heartbeat abhi <b>{cur}</b> hai.\nBadalne ke liye bhejein: <code>/heartbeat on</code> ya <code>/heartbeat off</code>")

        save_update_id(last_id)

    except Exception as e:
        log.error("Telegram command processing error: %s", e)

    return checked_in_cmd

# ─── REGISTER BOT COMMANDS WITH TELEGRAM ──────────────────────────────────
def setup_commands():
    try:
        cmds = [
            {"command": "status",    "description": "📊 Live bot status dekhein"},
            {"command": "check",     "description": "🔍 Site check karein"},
            {"command": "demo",      "description": "🧪 Website + Code ki saari PDFs scan karein"},
            {"command": "dl",        "description": "📥 Scanned PDF Telegram me mangwayein (/dl 1 or all)"},
            {"command": "heartbeat", "description": "🔔 Har 5-min status msg ON/OFF karein"},
            {"command": "help",      "description": "ℹ️ Help aur bot info"},
        ]
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
            json={"commands": cmds},
            timeout=10,
        )
    except Exception:
        pass

# ─── MAIN ─────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN not set! Add it as a GitHub Repository Secret.")
        sys.exit(1)

    log.info("🛩️  IAF Agniveervayu 01/2027 — GitHub Actions Monitor (60s Loop Mode)")
    log.info(
        "📡 Monitoring %d pages | Window: %ds | Check every %ds | curl_cffi: %s | PROXY_URL: %s",
        len(MONITOR_URLS),
        RUN_DURATION_SEC,
        CHECK_INTERVAL_SEC,
        "✅ Available" if HAS_CURL_CFFI else "❌ Missing",
        "✅ Configured" if PROXY_URL else "Not set (direct + web proxy fallbacks)",
    )

    chats = load_registered_chats()
    stats = load_stats()
    stats["total_runs"] += 1

    if not stats.get("commands_synced"):
        setup_commands()
        stats["commands_synced"] = True

    start_ts = time.monotonic()
    last_site_check = 0.0  # Triggers immediate check on first pass unless /check ran first
    sent_run_heartbeat = False

    while True:
        elapsed = time.monotonic() - start_ts
        remaining = RUN_DURATION_SEC - elapsed
        if remaining <= 1:
            break

        # 1. Poll Telegram commands/buttons immediately (4s long-poll)
        poll_sec = min(4, max(1, int(remaining)))
        checked_in_cmd = process_telegram_commands(chats, stats, poll_timeout=poll_sec)
        if checked_in_cmd:
            last_site_check = time.monotonic()

        # Re-check remaining budget before launching a website check
        elapsed = time.monotonic() - start_ts
        if RUN_DURATION_SEC - elapsed <= 5:
            break

        # 2. Check EDCIL every CHECK_INTERVAL_SEC seconds
        if last_site_check == 0.0 or (time.monotonic() - last_site_check) >= CHECK_INTERVAL_SEC:
            run_check(chats, stats, send_heartbeat=(not sent_run_heartbeat))
            sent_run_heartbeat = True
            last_site_check = time.monotonic()
            save_stats(stats)

    save_stats(stats)
    log.info("✅ Run #%d complete (%ds loop finished).", stats["total_runs"], int(time.monotonic() - start_ts))

if __name__ == "__main__":
    main()
