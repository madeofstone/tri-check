"""
Failure classification for AAC job groups.

Turns the platform's raw exception data into a category a human can act on.
Rules live in error_rules.json and are applied in order — first match wins —
so the file can be edited without touching this module.

Note: AAC's own `errorCategory` field is deliberately NOT used. It only maps
83 exception types to "user" and defaults everything else to "infrastructure",
which puts 99.8% of this tenant's failures in one bucket.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

RULES_FILE = Path(__file__).resolve().parent / "error_rules.json"

_rules: list[dict] = []
_fallback: dict = {}


def _compile(rule: dict) -> dict:
    out = dict(rule)
    out["_types"] = {t.lower() for t in rule.get("exception_type", [])}
    out["_labels"] = {l.lower() for l in rule.get("exception_label", [])}
    for key, target in (("why_regex", "_why"), ("message_regex", "_msg")):
        out[target] = re.compile(rule[key], re.I) if rule.get(key) else None
    return out


def load_rules(force: bool = False) -> list[dict]:
    """Load and compile the rule file (cached)."""
    global _rules, _fallback
    if _rules and not force:
        return _rules
    data = json.loads(RULES_FILE.read_text())
    _rules = [_compile(r) for r in data.get("rules", [])]
    _fallback = data.get("unclassified", {"id": "unclassified", "title": "Unclassified",
                                          "category": "Unclassified", "hint": ""})
    log.info(f"Loaded {len(_rules)} classification rules")
    return _rules


def parse_error_message(error_message: Optional[dict]) -> dict:
    """
    Flatten an AAC errorMessage record into the fields we classify on.

    The `message` field is a JSON *string* holding {"exception": {...}}, where
    details.why carries the human-readable cause when the platform provides one.
    """
    em = error_message or {}
    raw = em.get("message") or ""
    why, inner_message = None, None
    try:
        exc = (json.loads(raw) or {}).get("exception") or {}
        inner_message = exc.get("message")
        why = (exc.get("details") or {}).get("why")
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    # `why` and `message` are usually strings but the platform sometimes nests
    # an object there, so coerce before anything tries to regex over them.
    def _text(v):
        if v is None:
            return None
        return v if isinstance(v, str) else json.dumps(v)

    return {
        "errorMessageId": em.get("id"),
        "exceptionType": em.get("exceptionType"),
        "exceptionLabel": em.get("exceptionLabel"),
        "errorCode": em.get("errorCode"),
        "platformCategory": em.get("errorCategory"),
        "why": _text(why),
        "message": _text(inner_message) or raw,
        "rawMessage": raw if isinstance(raw, str) else json.dumps(raw),
    }


def classify(parsed: dict) -> dict:
    """Classify a parsed error. Returns {ruleId, title, category, hint}."""
    load_rules()
    etype = (parsed.get("exceptionType") or "").lower()
    elabel = (parsed.get("exceptionLabel") or "").lower()
    why = parsed.get("why") or ""
    blob = f"{parsed.get('message') or ''}\n{parsed.get('rawMessage') or ''}"

    for rule in _rules:
        if etype and etype in rule["_types"]:
            return _result(rule)
        if elabel and elabel in rule["_labels"]:
            return _result(rule)
        if rule["_why"] is not None and why and rule["_why"].search(why):
            return _result(rule)
        if rule["_msg"] is not None and rule["_msg"].search(blob):
            return _result(rule)
    return _result(_fallback)


def _result(rule: dict) -> dict:
    return {
        "ruleId": rule.get("id"),
        "title": rule.get("title"),
        "category": rule.get("category"),
        "hint": rule.get("hint"),
    }


def pick_root_failure(inner_jobs: list[dict]) -> Optional[dict]:
    """
    Choose the job that actually caused the group to fail.

    A failed group usually has several failed inner jobs: the real one plus
    UpstreamJobFailed cascades. 27% of failed inner jobs in the sampled corpus
    were cascades, so counting them all would badly skew the stage breakdown.
    Prefer a failed job whose error is not an upstream cascade, in pipeline
    order; fall back to the first failed job if every one is a cascade.
    """
    ORDER = {"ingest": 0, "schemadrift": 1, "wrangle": 2, "profile": 3,
             "filewriter": 4, "publish": 5}
    failed = [j for j in (inner_jobs or []) if (j.get("status") or "").lower() == "failed"]
    if not failed:
        return None
    failed.sort(key=lambda j: ORDER.get((j.get("jobType") or "").lower(), 99))
    for job in failed:
        etype = ((job.get("errorMessage") or {}).get("exceptionType") or "").lower()
        if etype != "upstreamjobfailed":
            return job
    return failed[0]
