#!/usr/bin/env python3
"""
archive_diagnostic.py - Diagnostic CLI tool for SQLite market archive database.
Inspects 'candles' and 'quotes' tables for schema, stats, logical duplicates,
null/non-numeric values, invalid OHLC relationships, timestamp parsing,
and unexpected time gaps.

No external dependencies (standard library sqlite3, json, sys, argparse, datetime, math).
"""

import argparse
import datetime
import json
import math
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}


def parse_iso_timestamp(ts_val: Any) -> Optional[datetime.datetime]:
    """Parse timestamp into UTC datetime. Handles ISO-8601 with Z, offsets, or numeric epoch."""
    if ts_val is None:
        return None
    if isinstance(ts_val, (int, float)):
        # Epoch timestamp in seconds or milliseconds
        if ts_val > 1e11:  # likely milliseconds
            ts_val = ts_val / 1000.0
        try:
            return datetime.datetime.fromtimestamp(ts_val, tz=datetime.timezone.utc)
        except Exception:
            return None

    if isinstance(ts_val, str):
        s = ts_val.strip()
        if not s:
            return None
        # Replace trailing Z with +00:00 for fromisoformat compatibility
        if s.endswith("Z") or s.endswith("z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            else:
                dt = dt.astimezone(datetime.timezone.utc)
            return dt
        except Exception:
            # Try parsing numeric string
            try:
                val = float(s)
                if val > 1e11:
                    val = val / 1000.0
                return datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc)
            except Exception:
                return None
    return None


def is_valid_number(val: Any) -> bool:
    """Check if value is a finite number (int/float)."""
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return not (math.isnan(val) or math.isinf(val))
    if isinstance(val, str):
        try:
            f = float(val)
            return not (math.isnan(f) or math.isinf(f))
        except ValueError:
            return False
    return False


def to_float(val: Any) -> Optional[float]:
    """Convert value to float if finite, else None."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def get_table_schema(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Fetch columns, types, and indexes for a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    cols = []
    for row in cursor.fetchall():
        cols.append({
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": bool(row[3]),
            "default_value": row[4],
            "pk": bool(row[5])
        })

    cursor.execute(f"PRAGMA index_list({table_name});")
    indexes = []
    for idx_row in cursor.fetchall():
        idx_name = idx_row[1]
        unique = bool(idx_row[2])
        cursor.execute(f"PRAGMA index_info({idx_name});")
        idx_cols = [r[2] for r in cursor.fetchall()]
        indexes.append({
            "name": idx_name,
            "unique": unique,
            "columns": idx_cols
        })

    return {
        "columns": cols,
        "indexes": indexes
    }


def analyze_candles(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze candles table for anomalies, invalid OHLC, gaps, and statistics."""
    cursor = conn.cursor()
    schema_info = get_table_schema(conn, "candles")
    col_names = [c["name"].lower() for c in schema_info["columns"]]

    # Mapping expected columns
    def find_col(possible_names: List[str]) -> Optional[str]:
        for p in possible_names:
            for actual in col_names:
                if actual == p:
                    return actual
        return None

    symbol_col = find_col(["symbol", "pair", "ticker"]) or "symbol"
    tf_col = find_col(["timeframe", "tf", "interval"]) or "timeframe"
    time_col = find_col(["timestamp", "time", "datetime", "date", "ts"]) or "timestamp"
    open_col = find_col(["open", "o"]) or "open"
    high_col = find_col(["high", "h"]) or "high"
    low_col = find_col(["low", "l"]) or "low"
    close_col = find_col(["close", "c"]) or "close"
    vol_col = find_col(["volume", "vol", "v"]) or "volume"

    # Total record count
    cursor.execute("SELECT COUNT(*) FROM candles;")
    total_count = cursor.fetchone()[0]

    if total_count == 0:
        return {
            "schema": schema_info,
            "total_records": 0,
            "symbols": [],
            "timeframes": [],
            "min_timestamp_utc": None,
            "max_timestamp_utc": None,
            "duplicate_count": 0,
            "unparseable_timestamps_count": 0,
            "null_or_invalid_numeric_count": 0,
            "invalid_ohlc_count": 0,
            "gaps": []
        }

    # Fetch all records to do comprehensive in-memory analysis
    query = f"SELECT rowid, {symbol_col}, {tf_col}, {time_col}, {open_col}, {high_col}, {low_col}, {close_col}, {vol_col} FROM candles"
    cursor.execute(query)
    rows = cursor.fetchall()

    duplicates_seen = set()
    duplicate_rows = []
    unparseable_timestamps = []
    null_or_invalid_numerics = []
    invalid_ohlc = []

    parsed_series: Dict[Tuple[str, str], List[Tuple[datetime.datetime, int]]] = {}
    valid_timestamps = []
    unique_symbols = set()
    unique_tfs = set()

    for row in rows:
        rowid, sym, tf, raw_ts, o, h, l, c, v = row
        sym_str = str(sym) if sym is not None else "UNKNOWN"
        tf_str = str(tf) if tf is not None else "UNKNOWN"
        unique_symbols.add(sym_str)
        unique_tfs.add(tf_str)

        # Logical key check: (symbol, timeframe, raw_ts)
        logical_key = (sym_str, tf_str, str(raw_ts))
        if logical_key in duplicates_seen:
            duplicate_rows.append({"rowid": rowid, "key": logical_key})
        else:
            duplicates_seen.add(logical_key)

        # Timestamp check
        dt = parse_iso_timestamp(raw_ts)
        if dt is None:
            unparseable_timestamps.append({"rowid": rowid, "symbol": sym_str, "timeframe": tf_str, "raw": raw_ts})
        else:
            valid_timestamps.append(dt)
            key = (sym_str, tf_str)
            if key not in parsed_series:
                parsed_series[key] = []
            parsed_series[key].append((dt, rowid))

        # Numeric validity check
        f_o, f_h, f_l, f_c, f_v = to_float(o), to_float(h), to_float(l), to_float(c), to_float(v)
        has_invalid_num = False
        for val_name, val in [("open", f_o), ("high", f_h), ("low", f_l), ("close", f_c), ("volume", f_v)]:
            if val is None:
                null_or_invalid_numerics.append({"rowid": rowid, "field": val_name, "raw": locals()[val_name[0]]})
                has_invalid_num = True

        # OHLC logic check
        if not has_invalid_num and f_o is not None and f_h is not None and f_l is not None and f_c is not None and f_v is not None:
            reasons = []
            max_oc = max(f_o, f_c)
            min_oc = min(f_o, f_c)
            if f_h < max_oc:
                reasons.append(f"high ({f_h}) < max(open, close) ({max_oc})")
            if f_l > min_oc:
                reasons.append(f"low ({f_l}) > min(open, close) ({min_oc})")
            if f_h < f_l:
                reasons.append(f"high ({f_h}) < low ({f_l})")
            if f_v < 0:
                reasons.append(f"volume ({f_v}) < 0")

            if reasons:
                invalid_ohlc.append({
                    "rowid": rowid,
                    "symbol": sym_str,
                    "timeframe": tf_str,
                    "timestamp": str(raw_ts),
                    "reasons": reasons
                })

    min_ts_str = min(valid_timestamps).isoformat() if valid_timestamps else None
    max_ts_str = max(valid_timestamps).isoformat() if valid_timestamps else None

    # Detect gaps
    gaps = []
    for (sym, tf), records in parsed_series.items():
        if len(records) < 2:
            continue
        # Sort by timestamp
        records.sort(key=lambda x: x[0])

        # Determine expected step
        expected_step_sec = TIMEFRAME_SECONDS.get(tf.lower())
        if expected_step_sec is None:
            # Estimate from median delta
            deltas = []
            for i in range(1, len(records)):
                diff = (records[i][0] - records[i - 1][0]).total_seconds()
                if diff > 0:
                    deltas.append(diff)
            if deltas:
                deltas.sort()
                mid = len(deltas) // 2
                expected_step_sec = deltas[mid] if len(deltas) % 2 != 0 else (deltas[mid - 1] + deltas[mid]) / 2.0
            else:
                expected_step_sec = 60.0

        if expected_step_sec <= 0:
            expected_step_sec = 60.0

        # Check for gaps (diff > 1.5 * expected_step)
        for i in range(1, len(records)):
            prev_dt, prev_id = records[i - 1]
            curr_dt, curr_id = records[i]
            diff = (curr_dt - prev_dt).total_seconds()
            if diff > expected_step_sec:
                gaps.append({
                    "symbol": sym,
                    "timeframe": tf,
                    "prev_timestamp": prev_dt.isoformat(),
                    "curr_timestamp": curr_dt.isoformat(),
                    "gap_seconds": diff,
                    "expected_step_seconds": expected_step_sec,
                    "missing_estimated_candles": int(diff / expected_step_sec) - 1
                })

    return {
        "schema": schema_info,
        "total_records": total_count,
        "symbols": sorted(list(unique_symbols)),
        "timeframes": sorted(list(unique_tfs)),
        "min_timestamp_utc": min_ts_str,
        "max_timestamp_utc": max_ts_str,
        "duplicate_count": len(duplicate_rows),
        "duplicates": duplicate_rows[:20],
        "unparseable_timestamps_count": len(unparseable_timestamps),
        "unparseable_timestamps": unparseable_timestamps[:20],
        "null_or_invalid_numeric_count": len(null_or_invalid_numerics),
        "null_or_invalid_numerics": null_or_invalid_numerics[:20],
        "invalid_ohlc_count": len(invalid_ohlc),
        "invalid_ohlc": invalid_ohlc[:20],
        "gap_count": len(gaps),
        "gaps": gaps[:20]
    }


def analyze_quotes(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Analyze quotes table if it exists."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='quotes';")
    if not cursor.fetchone():
        return None

    schema_info = get_table_schema(conn, "quotes")
    col_names = [c["name"].lower() for c in schema_info["columns"]]

    cursor.execute("SELECT COUNT(*) FROM quotes;")
    total_count = cursor.fetchone()[0]

    if total_count == 0:
        return {
            "schema": schema_info,
            "total_records": 0,
            "symbols": [],
            "min_timestamp_utc": None,
            "max_timestamp_utc": None,
            "duplicate_count": 0,
            "unparseable_timestamps_count": 0,
            "null_or_invalid_numeric_count": 0
        }

    # Find columns
    def find_col(possible_names: List[str]) -> Optional[str]:
        for p in possible_names:
            for actual in col_names:
                if actual == p:
                    return actual
        return None

    symbol_col = find_col(["symbol", "pair", "ticker"]) or "symbol"
    time_col = find_col(["timestamp", "time", "datetime", "date", "ts"]) or "timestamp"
    price_col = find_col(["price", "ask", "bid", "last", "val", "quote"]) or (col_names[2] if len(col_names) > 2 else "price")

    cursor.execute(f"SELECT rowid, {symbol_col}, {time_col}, {price_col} FROM quotes")
    rows = cursor.fetchall()

    duplicates_seen = set()
    duplicate_rows = []
    unparseable_timestamps = []
    null_or_invalid_numerics = []
    valid_timestamps = []
    unique_symbols = set()

    for row in rows:
        rowid, sym, raw_ts, price = row
        sym_str = str(sym) if sym is not None else "UNKNOWN"
        unique_symbols.add(sym_str)

        logical_key = (sym_str, str(raw_ts))
        if logical_key in duplicates_seen:
            duplicate_rows.append({"rowid": rowid, "key": logical_key})
        else:
            duplicates_seen.add(logical_key)

        dt = parse_iso_timestamp(raw_ts)
        if dt is None:
            unparseable_timestamps.append({"rowid": rowid, "symbol": sym_str, "raw": raw_ts})
        else:
            valid_timestamps.append(dt)

        if not is_valid_number(price):
            null_or_invalid_numerics.append({"rowid": rowid, "field": price_col, "raw": price})

    min_ts = min(valid_timestamps).isoformat() if valid_timestamps else None
    max_ts = max(valid_timestamps).isoformat() if valid_timestamps else None

    return {
        "schema": schema_info,
        "total_records": total_count,
        "symbols": sorted(list(unique_symbols)),
        "min_timestamp_utc": min_ts,
        "max_timestamp_utc": max_ts,
        "duplicate_count": len(duplicate_rows),
        "duplicates": duplicate_rows[:20],
        "unparseable_timestamps_count": len(unparseable_timestamps),
        "unparseable_timestamps": unparseable_timestamps[:20],
        "null_or_invalid_numeric_count": len(null_or_invalid_numerics),
        "null_or_invalid_numerics": null_or_invalid_numerics[:20]
    }


def print_text_report(db_path: str, candles_res: Optional[Dict[str, Any]], quotes_res: Optional[Dict[str, Any]]) -> None:
    """Print human-readable diagnostic report."""
    print("=" * 70)
    print(f" MARKET ARCHIVE DIAGNOSTIC REPORT: {os.path.abspath(db_path)}")
    print("=" * 70)

    if candles_res is None:
        print("[!] Table 'candles' NOT FOUND in database.")
    else:
        print("\n--- TABLE: candles ---")
        print(f"Total records:       {candles_res['total_records']}")
        print(f"Symbols ({len(candles_res['symbols'])}):        {', '.join(candles_res['symbols']) if candles_res['symbols'] else 'None'}")
        print(f"Timeframes ({len(candles_res['timeframes'])}):     {', '.join(candles_res['timeframes']) if candles_res['timeframes'] else 'None'}")
        print(f"Time range (UTC):    {candles_res['min_timestamp_utc']} -> {candles_res['max_timestamp_utc']}")
        print(f"Logical duplicates:  {candles_res['duplicate_count']}")
        print(f"Unparseable dates:   {candles_res['unparseable_timestamps_count']}")
        print(f"Null/NaN/Inf numbers:{candles_res['null_or_invalid_numeric_count']}")
        print(f"Invalid OHLC rules:  {candles_res['invalid_ohlc_count']}")
        print(f"Time gaps detected:  {candles_res['gap_count']}")

        print("\nColumns:")
        for col in candles_res["schema"]["columns"]:
            pk_str = " [PK]" if col["pk"] else ""
            nn_str = " NOT NULL" if col["notnull"] else ""
            print(f"  - {col['name']} ({col['type']}){pk_str}{nn_str}")

        print("Indexes:")
        if candles_res["schema"]["indexes"]:
            for idx in candles_res["schema"]["indexes"]:
                u_str = "UNIQUE " if idx["unique"] else ""
                print(f"  - {idx['name']}: {u_str}({', '.join(idx['columns'])})")
        else:
            print("  (No indexes found)")

        if candles_res["invalid_ohlc_count"] > 0:
            print(f"\nSample Invalid OHLC (first {len(candles_res['invalid_ohlc'])}):")
            for item in candles_res["invalid_ohlc"]:
                print(f"  Row {item['rowid']} [{item['symbol']} {item['timeframe']} @ {item['timestamp']}]: {'; '.join(item['reasons'])}")

        if candles_res["gap_count"] > 0:
            print(f"\nSample Gaps (first {len(candles_res['gaps'])}):")
            for g in candles_res["gaps"]:
                print(f"  [{g['symbol']} {g['timeframe']}] {g['prev_timestamp']} -> {g['curr_timestamp']} (gap: {g['gap_seconds']}s, ~{g['missing_estimated_candles']} bars missing)")

    if quotes_res is not None:
        print("\n--- TABLE: quotes ---")
        print(f"Total records:       {quotes_res['total_records']}")
        print(f"Symbols ({len(quotes_res['symbols'])}):        {', '.join(quotes_res['symbols']) if quotes_res['symbols'] else 'None'}")
        print(f"Time range (UTC):    {quotes_res['min_timestamp_utc']} -> {quotes_res['max_timestamp_utc']}")
        print(f"Logical duplicates:  {quotes_res['duplicate_count']}")
        print(f"Unparseable dates:   {quotes_res['unparseable_timestamps_count']}")
        print(f"Null/NaN/Inf numbers:{quotes_res['null_or_invalid_numeric_count']}")

        print("\nColumns:")
        for col in quotes_res["schema"]["columns"]:
            pk_str = " [PK]" if col["pk"] else ""
            nn_str = " NOT NULL" if col["notnull"] else ""
            print(f"  - {col['name']} ({col['type']}){pk_str}{nn_str}")

    print("=" * 70)


def main() -> int:
    parser = argparse.ArgumentParser(description="Market Archive SQLite Diagnostic Tool")
    parser.add_argument("database", nargs="?", default="market_archive.sqlite3", help="Path to SQLite database file (default: market_archive.sqlite3)")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    args = parser.parse_args()

    db_path = args.database

    if not os.path.exists(db_path):
        err_msg = f"Error: Database file '{db_path}' not found."
        if args.json:
            print(json.dumps({"error": err_msg, "database": db_path}))
        else:
            print(err_msg, file=sys.stderr)
        return 1

    try:
        conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        # Fallback to regular connect if URI mode fails
        try:
            conn = sqlite3.connect(db_path)
        except Exception as e:
            err_msg = f"Error connecting to database '{db_path}': {e}"
            if args.json:
                print(json.dumps({"error": err_msg, "database": db_path}))
            else:
                print(err_msg, file=sys.stderr)
            return 1

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='candles';")
        has_candles = cursor.fetchone() is not None

        candles_res = analyze_candles(conn) if has_candles else None
        quotes_res = analyze_quotes(conn)

        report = {
            "database": os.path.abspath(db_path),
            "candles": candles_res,
            "quotes": quotes_res
        }

        if args.json:
            print(json.dumps(report, indent=2))
        else:
            print_text_report(db_path, candles_res, quotes_res)

        return 0
    except Exception as e:
        err_msg = f"Diagnostic execution error: {e}"
        if args.json:
            print(json.dumps({"error": err_msg, "database": db_path}))
        else:
            print(err_msg, file=sys.stderr)
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
