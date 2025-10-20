# analytics_s3.py
import os, json, uuid, datetime as dt
from typing import Dict, Iterable, Tuple
from flask import Blueprint, request, jsonify

import boto3
from botocore.exceptions import ClientError

analytics_bp = Blueprint("analytics", __name__, url_prefix="/api")

# ---------- Config (set in Replit Secrets) ----------
S3_BUCKET = os.environ["ANALYTICS_S3_BUCKET"]           # REQUIRED
S3_PREFIX = os.getenv("ANALYTICS_S3_PREFIX", "events")  # e.g. "events"
# Partitioning style: events/yyyy/mm/dd/hh/<uuid>.json
# You may also choose minute-level if you expect very high volumes.

s3 = boto3.client("s3")

def _now():
    return dt.datetime.utcnow().replace(microsecond=0)

def _now_iso():
    return _now().isoformat() + "Z"

def _hour_prefix(ts: dt.datetime) -> str:
    return f"{S3_PREFIX}/{ts.year:04d}/{ts.month:02d}/{ts.day:02d}/{ts.hour:02d}/"

def _hourly_prefixes_between(since: dt.datetime, until: dt.datetime) -> Iterable[str]:
    """Generate hour partitions covering [since, until]."""
    cur = since.replace(minute=0, second=0, microsecond=0)
    end = until.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        yield _hour_prefix(cur)
        cur += dt.timedelta(hours=1)

def _parse_window(window: str) -> int:
    window = (window or "7d").lower()
    if window.endswith("h"):
        return max(1, int(window[:-1]))
    if window.endswith("d"):
        return max(1, int(window[:-1]) * 24)
    return 24*7

def _safe_json(obj) -> bytes:
    try:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")
    except Exception:
        return b"{}"

# ------------- Write: /api/track -------------
@analytics_bp.route("/track", methods=["POST"])
def track():
    """
    Body JSON:
    {
      "user_id": "abc123" (optional; if missing, server derives anon id),
      "module": "earnings" | "news" | "valuation" | "data" | "fda",
      "action": "open" | "click" | "query" | "submit" | ...,
      "meta": {...}    # optional
    }
    """
    data = request.get_json(silent=True) or {}
    module = (data.get("module") or "").strip().lower()
    action = (data.get("action") or "").strip().lower()
    meta = data.get("meta") or {}

    # Resolve user_id priority: explicit > header > cookie > ip/UA hash
    import uuid as _uuid
    user_id = (data.get("user_id") or request.headers.get("X-User-Id") or "").strip()
    if not user_id:
        user_id = request.cookies.get("cid", "")
    if not user_id:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
        ua = request.headers.get("User-Agent", "")
        user_id = f"anon_{_uuid.uuid5(_uuid.NAMESPACE_DNS, f'{ip}|{ua}')}"

    if not module or not action:
        return jsonify({"ok": False, "error": "module and action are required"}), 400

    ts = _now()
    event = {
        "id": str(uuid.uuid4()),
        "ts_utc": ts.isoformat() + "Z",
        "user_id": user_id[:128],
        "module": module[:64],
        "action": action[:64],
        "meta": meta or {},
        "ip": request.headers.get("X-Forwarded-For", request.remote_addr or ""),
        "user_agent": request.headers.get("User-Agent", ""),
    }

    key = f"{_hour_prefix(ts)}{event['id']}.json"
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=_safe_json(event))
    except ClientError as e:
        return jsonify({"ok": False, "error": f"S3 put failed: {e}"}), 500

    return jsonify({"ok": True, "id": event["id"], "ts_utc": event["ts_utc"]})

# ---------- Helpers to read & aggregate ----------
def _list_event_keys(since: dt.datetime, until: dt.datetime) -> Iterable[str]:
    """List S3 object keys under hour partitions for [since, until]."""
    for prefix in _hourly_prefixes_between(since, until):
        continuation = None
        while True:
            kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
            if continuation: kwargs["ContinuationToken"] = continuation
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                yield obj["Key"]
            if resp.get("IsTruncated"):
                continuation = resp["NextContinuationToken"]
            else:
                break

def _load_event(key: str) -> Dict:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    b = obj["Body"].read()
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {}

def _aggregate(keys: Iterable[str], module_filter: str = None) -> Tuple[Dict, Dict]:
    """
    Returns:
      by_group: dict for /stats
      daily: dict for /timeseries  -> { 'YYYY-MM-DD': {module: count} }
    """
    by_group: Dict[Tuple, int] = {}
    users_by_group: Dict[Tuple, set] = {}
    daily: Dict[str, Dict[str, int]] = {}

    for k in keys:
        ev = _load_event(k)
        if not ev: continue
        mod = ev.get("module") or "unknown"
        if module_filter and mod != module_filter:  # exact match after lowercasing in write path
            continue
        ts = ev.get("ts_utc", "")[:10]  # YYYY-MM-DD
        uid = ev.get("user_id", "na")
        # group by module and user/module combos based on later query
        # We'll compute flexible groupings in route by reshaping this map.

        # daily timeseries
        daily.setdefault(ts, {})
        daily[ts][mod] = daily[ts].get(mod, 0) + 1

        # store raw for flexible regroup later
        g = (mod, uid)
        by_group[g] = by_group.get(g, 0) + 1
        users_by_group.setdefault((mod,), set()).add(uid)

    return by_group, daily

# ------------- Read: /api/stats -------------
@analytics_bp.route("/stats", methods=["GET"])
def stats():
    """
    Query params:
      window: 24h|7d|30d|90d (default 7d)
      by: 'module' | 'user' | 'module,user' (default 'module')
      module: optional exact filter (e.g., 'earnings')
    """
    window = request.args.get("window", "7d")
    by = request.args.get("by", "module")
    module_filter = request.args.get("module")
    if module_filter: module_filter = module_filter.lower()

    hours = _parse_window(window)
    until = _now()
    since = until - dt.timedelta(hours=hours)

    keys = list(_list_event_keys(since, until))
    by_group_raw, _daily = _aggregate(keys, module_filter=module_filter)

    # reshape into requested grouping
    results = []
    if by == "module":
        # aggregate counts and unique users per module
        per_mod = {}
        users = {}
        for (mod, uid), cnt in by_group_raw.items():
            per_mod[mod] = per_mod.get(mod, 0) + cnt
            users.setdefault(mod, set()).add(uid)
        for mod, cnt in sorted(per_mod.items(), key=lambda x: x[1], reverse=True)[:500]:
            results.append({"module": mod, "events": cnt, "users": len(users.get(mod, set()))})
    elif by == "user":
        # total per user (across modules)
        per_user = {}
        for (_mod, uid), cnt in by_group_raw.items():
            per_user[uid] = per_user.get(uid, 0) + cnt
        for uid, cnt in sorted(per_user.items(), key=lambda x: x[1], reverse=True)[:500]:
            results.append({"user_id": uid, "events": cnt})
    else:  # "module,user"
        for (mod, uid), cnt in sorted(by_group_raw.items(), key=lambda x: x[1], reverse=True)[:500]:
            results.append({"module": mod, "user_id": uid, "events": cnt})

    return jsonify({
        "ok": True,
        "window": window,
        "since_utc": since.isoformat() + "Z",
        "until_utc": until.isoformat() + "Z",
        "group_by": by,
        "results": results
    })

# ------------- Read: /api/timeseries -------------
@analytics_bp.route("/timeseries", methods=["GET"])
def timeseries():
    """
    Simple per-module daily counts.
    Query params:
      window: default 30d
      module: optional exact filter
    """
    window = request.args.get("window", "30d")
    module_filter = request.args.get("module")
    if module_filter: module_filter = module_filter.lower()

    hours = _parse_window(window)
    until = _now()
    since = until - dt.timedelta(hours=hours)

    keys = list(_list_event_keys(since, until))
    _by_group, daily = _aggregate(keys, module_filter=module_filter)
    return jsonify({
        "ok": True,
        "since_utc": since.isoformat() + "Z",
        "until_utc": until.isoformat() + "Z",
        "series": daily  # { 'YYYY-MM-DD': {module: count} }
    })
