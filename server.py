#!/usr/bin/env python3
"""CoinMarketCap REST proxy, persistent market archive, and static dashboard server.
No synthetic market data; only verified API payloads and persistent SQLite archive.
"""
import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "market_archive.sqlite3"
CMC_API_KEY = os.environ.get("CMC_API_KEY", "").strip()
CMC_BASE = "https://pro-api.coinmarketcap.com"
TTL = 30
CACHE = {}
CACHE_LOCK = threading.Lock()
DEFAULTS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "TRX"]
INTERVALS = {"1d": "daily", "4h": "4h", "1h": "1h"}

# Configurable background polling interval (minimum 30 seconds, default 60)
try:
    MARKET_POLL_SECONDS = max(30, int(os.environ.get("MARKET_POLL_SECONDS", "60")))
except (ValueError, TypeError):
    MARKET_POLL_SECONDS = 60

LAST_POLL_STATUS = {
    "last_run": None,
    "last_success": None,
    "last_error": None,
    "running": False,
    "poll_interval_seconds": MARKET_POLL_SECONDS,
}


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=15.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create persistent archive tables and indexes."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT,
                price REAL NOT NULL,
                percent_change_24h REAL,
                market_cap REAL,
                volume_24h REAL,
                quote_timestamp TEXT,
                fetched_at INTEGER NOT NULL,
                source TEXT DEFAULT 'coinmarketcap',
                raw_json TEXT,
                UNIQUE(symbol, quote_timestamp)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                time TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL DEFAULT 0,
                fetched_at INTEGER NOT NULL,
                source TEXT DEFAULT 'coinmarketcap',
                UNIQUE(symbol, interval, time)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_sym_time ON quotes(symbol, fetched_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_sym_int_time ON candles(symbol, interval, time ASC)")
        conn.commit()


def archive_market_data(payload):
    """Store live quotes and candles safely into SQLite archive without duplicate rows."""
    if not isinstance(payload, dict) or not payload.get("live"):
        return 0, 0
    fetched_at = int(payload.get("fetched_at") or time.time())
    assets = payload.get("assets") or {}
    candles_dict = payload.get("candles") or {}
    interval = payload.get("effective_interval") or payload.get("requested_interval") or "1d"

    quotes_saved = 0
    candles_saved = 0

    try:
        with get_db() as conn:
            for symbol, asset in assets.items():
                price = asset.get("price")
                if price is not None and __import__("math").isfinite(float(price)):
                    ts = asset.get("quote_timestamp") or str(fetched_at)
                    try:
                        conn.execute("""
                            INSERT INTO quotes (symbol, name, price, percent_change_24h, market_cap, volume_24h, quote_timestamp, fetched_at, source, raw_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(symbol, quote_timestamp) DO UPDATE SET
                                price=excluded.price,
                                percent_change_24h=excluded.percent_change_24h,
                                market_cap=excluded.market_cap,
                                volume_24h=excluded.volume_24h,
                                fetched_at=excluded.fetched_at
                        """, (
                            symbol,
                            asset.get("name") or symbol,
                            float(price),
                            asset.get("percent_change_24h"),
                            asset.get("market_cap"),
                            asset.get("volume_24h"),
                            ts,
                            fetched_at,
                            asset.get("source") or "coinmarketcap",
                            json.dumps(asset, ensure_ascii=False),
                        ))
                        quotes_saved += 1
                    except sqlite3.Error:
                        pass

            for symbol, c_list in candles_dict.items():
                if not isinstance(c_list, list):
                    continue
                for c in c_list:
                    c_time = c.get("time")
                    if not c_time:
                        continue
                    try:
                        o = float(c["open"])
                        h = float(c["high"])
                        l = float(c["low"])
                        cl = float(c["close"])
                        v = float(c.get("volume") or 0)
                        if not all(__import__("math").isfinite(x) for x in (o, h, l, cl, v)):
                            continue
                        conn.execute("""
                            INSERT INTO candles (symbol, interval, time, open, high, low, close, volume, fetched_at, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(symbol, interval, time) DO UPDATE SET
                                open=excluded.open,
                                high=excluded.high,
                                low=excluded.low,
                                close=excluded.close,
                                volume=excluded.volume,
                                fetched_at=excluded.fetched_at
                        """, (
                            symbol,
                            interval,
                            str(c_time),
                            o, h, l, cl, v,
                            fetched_at,
                            "coinmarketcap",
                        ))
                        candles_saved += 1
                    except (KeyError, TypeError, ValueError, sqlite3.Error):
                        continue
            conn.commit()
    except Exception as exc:
        LAST_POLL_STATUS["last_error"] = f"Archive write error: {exc}"
        return quotes_saved, candles_saved

    return quotes_saved, candles_saved


def get_archive_stats():
    """Return database statistics for monitoring."""
    try:
        with get_db() as conn:
            q_count = conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
            c_count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
            syms_row = conn.execute("SELECT COUNT(DISTINCT symbol) FROM quotes").fetchone()
            syms_count = syms_row[0] if syms_row else 0
            latest_quote = conn.execute("SELECT MAX(fetched_at) FROM quotes").fetchone()[0]
            latest_candle = conn.execute("SELECT MAX(fetched_at) FROM candles").fetchone()[0]
            return {
                "quotes_count": q_count,
                "candles_count": c_count,
                "symbols_count": syms_count,
                "latest_quote_fetched_at": latest_quote,
                "latest_candle_fetched_at": latest_candle,
                "database_file": DB_PATH.name,
                "database_size_bytes": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
                "poll_interval_seconds": MARKET_POLL_SECONDS,
                "last_poll_run": LAST_POLL_STATUS.get("last_run"),
                "last_poll_success": LAST_POLL_STATUS.get("last_success"),
                "last_poll_error": LAST_POLL_STATUS.get("last_error"),
            }
    except Exception as exc:
        return {
            "error": str(exc),
            "database_file": DB_PATH.name,
            "poll_interval_seconds": MARKET_POLL_SECONDS,
        }


def cmc_get(path, params=None, timeout=10):
    url = CMC_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={
        "X-CMC_PRO_API_KEY": CMC_API_KEY,
        "Accept": "application/json",
        "User-Agent": "NabzCrypto/3.0",
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def error_text(exc):
    if isinstance(exc, urllib.error.HTTPError):
        try:
            body = json.loads(exc.read().decode("utf-8"))
            detail = body.get("status", {}).get("error_message", "")
        except Exception:
            detail = ""
        return f"CoinMarketCap HTTP {exc.code}" + (f": {detail}" if detail else ""), exc.code
    if isinstance(exc, (TimeoutError, urllib.error.URLError)):
        reason = getattr(exc, "reason", exc)
        return f"CoinMarketCap connection/timeout error: {reason}", None
    return f"CoinMarketCap response error: {exc}", None


def extract_candles(payload):
    """Parse documented CMC historical OHLCV quotes; discard malformed rows."""
    data = payload.get("data") or {}
    rows = []
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, dict) and isinstance(value.get("quotes"), list):
                rows = value["quotes"]
                break
            if isinstance(value, list):
                rows = value
                break
        if not rows and isinstance(data.get("quotes"), list):
            rows = data["quotes"]
    elif isinstance(data, list):
        rows = data
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        quote = row.get("quote", {})
        if isinstance(quote, list):
            quote = quote[0] if quote else {}
        if not isinstance(quote, dict):
            quote = {}
        try:
            candle = {
                "time": row.get("time_open") or row.get("timestamp") or row.get("time_close"),
                "open": float(quote["open"]), "high": float(quote["high"]),
                "low": float(quote["low"]), "close": float(quote["close"]),
                "volume": float(quote.get("volume") or 0),
            }
            if candle["time"] and all(__import__("math").isfinite(candle[k]) for k in ("open", "high", "low", "close", "volume")):
                result.append(candle)
        except (KeyError, TypeError, ValueError):
            continue
    return result


def fetch_market(symbols, interval):
    now = int(time.time())
    result = {
        "source": "coinmarketcap",
        "status": "error",
        "live": False,
        "fetched_at": now,
        "requested_interval": interval,
        "effective_interval": None,
        "symbols": symbols,
        "assets": {},
        "candles": {},
        "errors": [],
        "cached": False,
        "archive_status": get_archive_stats(),
        "poll_interval_seconds": MARKET_POLL_SECONDS,
        "note": "CMC REST داده‌ای با تأخیر احتمالی است و فید وب‌سوکت/تیک‌به‌تیک نیست.",
    }
    if not CMC_API_KEY or CMC_API_KEY == "missing":
        result.update(source="none", status="needs_api_key")
        result["errors"].append("کلید معتبر در متغیر محیطی CMC_API_KEY تنظیم نشده است؛ هیچ داده‌ای ساخته یا شبیه‌سازی نشد.")
        for symbol in symbols:
            result["assets"][symbol] = {"symbol": symbol, "source": "none", "error": "نیازمند تنظیم API"}
        return result

    quotes = {}
    try:
        payload = cmc_get("/v1/cryptocurrency/quotes/latest", {"symbol": ",".join(symbols), "convert": "USD"})
        quotes = payload.get("data", {})
    except Exception as exc:
        message, code = error_text(exc)
        result["errors"].append("دریافت قیمت: " + message)
        if code in (401, 403):
            result["errors"].append("کلید API یا سطح اشتراک CMC را بررسی کنید.")
        if code == 429:
            result["errors"].append("محدودیت نرخ درخواست CMC؛ کمی بعد تلاش کنید.")

    ids = {}
    for symbol in symbols:
        item = quotes.get(symbol)
        if isinstance(item, list):
            item = item[0] if item else None
        if not isinstance(item, dict):
            result["assets"][symbol] = {"symbol": symbol, "source": "coinmarketcap", "error": "قیمت این نماد از CMC دریافت نشد."}
            continue
        usd = (item.get("quote") or {}).get("USD") or {}
        ids[symbol] = item.get("id")
        result["assets"][symbol] = {
            "id": item.get("id"), "symbol": symbol, "name": item.get("name", symbol),
            "price": usd.get("price"), "percent_change_24h": usd.get("percent_change_24h"),
            "market_cap": usd.get("market_cap"), "volume_24h": usd.get("volume_24h"),
            "quote_timestamp": usd.get("last_updated") or item.get("last_updated"),
            "source": "coinmarketcap", "fetched_at": now, "error": None,
        }

    for symbol in symbols:
        asset = result["assets"].get(symbol, {})
        coin_id = ids.get(symbol)
        if not coin_id:
            asset["candle_error"] = "شناسهٔ CMC برای نماد پیدا نشد؛ کندل دریافت نشد."
            continue
        try:
            hist = cmc_get("/v2/cryptocurrency/ohlcv/historical", {
                "id": coin_id, "interval": INTERVALS[interval], "count": 100, "convert": "USD"
            })
            parsed = extract_candles(hist)
            if parsed:
                result["candles"][symbol] = parsed
                asset.update(candle_interval=interval, candle_count=len(parsed),
                             candle_timestamp=parsed[-1]["time"], candle_error=None)
            else:
                asset["candle_error"] = "CMC برای این بازه کندل قابل‌استفاده‌ای برنگرداند؛ ممکن است بازه یا دسترسی پلن پشتیبانی نشود."
        except Exception as exc:
            message, code = error_text(exc)
            asset["candle_error"] = message
            if code in (401, 402, 403):
                asset["candle_error"] += " (دسترسی OHLCV یا این بازه ممکن است در پلن موجود نباشد.)"
            elif code == 429:
                asset["candle_error"] += " (محدودیت نرخ درخواست.)"

    for symbol, asset in result["assets"].items():
        asset.setdefault("candle_interval", interval)
        asset.setdefault("candle_count", len(result["candles"].get(symbol, [])))
        asset.setdefault("candle_timestamp", None)
        asset.setdefault("fetched_at", now)
        asset.setdefault("source", "coinmarketcap")
        if asset.get("price") is not None and symbol in result["candles"]:
            asset["live"] = True
        else:
            asset["live"] = False

    live_count = sum(1 for a in result["assets"].values() if a.get("live"))
    result["effective_interval"] = interval if live_count else None
    result["live"] = live_count > 0
    result["status"] = "success" if live_count == len(symbols) else "partial" if live_count else "error"

    # Automatically archive successful data
    if result["live"]:
        archive_market_data(result)
        result["archive_status"] = get_archive_stats()

    return result


def background_poller():
    """Periodic worker to keep archive up-to-date and cache warm."""
    time.sleep(2)
    while True:
        try:
            if CMC_API_KEY and CMC_API_KEY != "missing":
                now_ts = int(time.time())
                LAST_POLL_STATUS["last_run"] = now_ts
                payload = fetch_market(DEFAULTS, "1d")
                with CACHE_LOCK:
                    CACHE[(tuple(DEFAULTS), "1d")] = (time.time(), payload)
                if payload.get("live"):
                    LAST_POLL_STATUS["last_success"] = now_ts
                    LAST_POLL_STATUS["last_error"] = None
                else:
                    LAST_POLL_STATUS["last_error"] = " ".join(payload.get("errors") or ["No live data returned"])
        except Exception as exc:
            LAST_POLL_STATUS["last_error"] = str(exc)
        time.sleep(MARKET_POLL_SECONDS)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        origin = self.headers.get("Origin", "")
        if origin.startswith(("http://localhost", "http://127.0.0.1", "https://localhost")):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/archive/stats":
            self.send_json(200, {
                "source": "sqlite_archive",
                "status": "success",
                "stats": get_archive_stats(),
            })
            return

        if path == "/api/archive":
            query = urllib.parse.parse_qs(parsed.query)
            symbol = query.get("symbol", ["BTC"])[0].strip().upper()
            interval = query.get("interval", ["1d"])[0].lower()
            try:
                limit = min(2000, max(1, int(query.get("limit", [500])[0])))
            except ValueError:
                limit = 500

            try:
                with get_db() as conn:
                    # Retrieve archived candles
                    c_rows = conn.execute("""
                        SELECT time, open, high, low, close, volume, fetched_at, source
                        FROM candles
                        WHERE symbol = ? AND interval = ?
                        ORDER BY time ASC
                        LIMIT ?
                    """, (symbol, interval, limit)).fetchall()

                    candles = [{
                        "time": r["time"],
                        "open": r["open"],
                        "high": r["high"],
                        "low": r["low"],
                        "close": r["close"],
                        "volume": r["volume"],
                        "fetched_at": r["fetched_at"],
                        "source": r["source"],
                    } for r in c_rows]

                    # Retrieve recent quote history
                    q_rows = conn.execute("""
                        SELECT price, percent_change_24h, market_cap, volume_24h, quote_timestamp, fetched_at
                        FROM quotes
                        WHERE symbol = ?
                        ORDER BY fetched_at DESC
                        LIMIT 50
                    """, (symbol,)).fetchall()

                    quote_history = [{
                        "price": r["price"],
                        "percent_change_24h": r["percent_change_24h"],
                        "market_cap": r["market_cap"],
                        "volume_24h": r["volume_24h"],
                        "quote_timestamp": r["quote_timestamp"],
                        "fetched_at": r["fetched_at"],
                    } for r in q_rows]

                latest_quote = quote_history[0] if quote_history else None
                self.send_json(200, {
                    "source": "sqlite_archive",
                    "status": "success" if candles or quote_history else "empty",
                    "mode": "archive",
                    "symbol": symbol,
                    "interval": interval,
                    "candle_count": len(candles),
                    "candles": candles,
                    "latest_quote": latest_quote,
                    "quote_history": quote_history,
                    "archive_stats": get_archive_stats(),
                    "note": "این داده‌ها مستقیماً از آرشیو محلی پایگاه داده بازیابی شده‌اند و شبیه‌سازی نشده‌اند.",
                })
            except Exception as exc:
                self.send_json(500, {
                    "source": "sqlite_archive",
                    "status": "error",
                    "error": str(exc),
                    "symbol": symbol,
                    "candles": [],
                })
            return

        if path != "/api/market":
            self.path = "/index.html" if parsed.path == "/" else parsed.path
            return super().do_GET()

        query = urllib.parse.parse_qs(parsed.query)
        symbols = list(dict.fromkeys(s.strip().upper() for s in query.get("symbols", [",".join(DEFAULTS)])[0].split(",") if s.strip()))[:30]
        symbols = symbols or DEFAULTS
        interval = query.get("interval", ["1d"])[0].lower()
        if interval not in INTERVALS:
            self.send_json(400, {
                "source": "server", "status": "error", "live": False,
                "errors": ["interval باید یکی از 1d، 4h یا 1h باشد."], "assets": {}, "candles": {}, "requested_interval": interval
            })
            return

        key = (tuple(symbols), interval)
        with CACHE_LOCK:
            cached = CACHE.get(key)
        now = time.time()
        if cached and now - cached[0] < TTL:
            payload = dict(cached[1])
            payload["cached"] = True
            payload["fetched_at"] = int(now)
            payload["archive_status"] = get_archive_stats()
            self.send_json(200, payload)
            return

        payload = fetch_market(symbols, interval)
        with CACHE_LOCK:
            CACHE[key] = (now, payload)
        self.send_json(200, payload)

    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def main():
    init_db()
    port = int(os.environ.get("PORT", "8080"))
    
    # Start background polling thread
    poller = threading.Thread(target=background_poller, daemon=True, name="MarketPoller")
    poller.start()

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Dashboard: http://localhost:{port} | CMC API key: {'configured' if CMC_API_KEY else 'missing'} | Polling: {MARKET_POLL_SECONDS}s")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
