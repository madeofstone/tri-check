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
_advisory: dict = {}


def _compile(rule: dict) -> dict:
    out = dict(rule)
    out["_types"] = {t.lower() for t in rule.get("exception_type", [])}
    out["_labels"] = {l.lower() for l in rule.get("exception_label", [])}
    for key, target in (("why_regex", "_why"), ("message_regex", "_msg")):
        out[target] = re.compile(rule[key], re.I) if rule.get(key) else None
    return out


def load_rules(force: bool = False) -> list[dict]:
    """Load and compile the rule file (cached)."""
    global _rules, _fallback, _advisory
    if _rules and not force:
        return _rules
    data = json.loads(RULES_FILE.read_text())
    _rules = [_compile(r) for r in data.get("rules", [])]
    _fallback = data.get("unclassified", {"id": "unclassified", "title": "Unclassified",
                                          "category": "Unclassified", "hint": ""})
    _advisory = data.get("advisory", {"id": "schema_drift", "title": "Schema drift reported",
                                      "category": "Schema drift", "hint": ""})
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


def advisory_result() -> dict:
    """Classification for a group whose only failure was an advisory stage."""
    load_rules()
    return _result(_advisory)


def _result(rule: dict) -> dict:
    return {
        "ruleId": rule.get("id"),
        "title": rule.get("title"),
        "category": rule.get("category"),
        "hint": rule.get("hint"),
    }


ADVISORY_STAGES = {"schemadrift"}
"""
Stages whose failure does not stop the pipeline.

Schema drift only reports whether the source schema changed; the transformation
runs regardless. Treating its failure as the cause of a job group hides the real
one — in every sampled case where drift failed, the wrangle stage had also
failed with its own independent error.
"""


def has_advisory_failure(inner_jobs: list[dict]) -> bool:
    """Whether any advisory stage failed, regardless of what caused the group to fail."""
    return any((j.get("jobType") or "").lower() in ADVISORY_STAGES
               and (j.get("status") or "").lower() == "failed"
               for j in (inner_jobs or []))


def pick_root_failure(inner_jobs: list[dict], include_advisory: bool = True) -> Optional[dict]:
    """
    Choose the job that actually caused the group to fail.

    A failed group usually has several failed inner jobs. Two kinds are not the
    cause and are skipped when anything else failed:

      * UpstreamJobFailed cascades — 27% of failed inner jobs in the sampled
        corpus, and always a consequence of an earlier stage.
      * advisory stages (schema drift) — non-blocking by design.

    Only when an advisory stage is the *sole* failure is it returned as the
    cause, and then `include_advisory` lets callers opt out entirely.
    """
    ORDER = {"ingest": 0, "schemadrift": 1, "wrangle": 2, "profile": 3,
             "filewriter": 4, "publish": 5}
    failed = [j for j in (inner_jobs or []) if (j.get("status") or "").lower() == "failed"]
    if not failed:
        return None
    failed.sort(key=lambda j: ORDER.get((j.get("jobType") or "").lower(), 99))

    def is_cascade(job) -> bool:
        etype = ((job.get("errorMessage") or {}).get("exceptionType") or "").lower()
        return etype == "upstreamjobfailed"

    def is_advisory(job) -> bool:
        return (job.get("jobType") or "").lower() in ADVISORY_STAGES

    for job in failed:
        if not is_cascade(job) and not is_advisory(job):
            return job
    # Nothing but cascades and advisory failures left.
    for job in failed:
        if not is_cascade(job):
            return job if include_advisory else None
    return failed[0] if include_advisory else None


# ---------------------------------------------------------------------------
# Message signatures
# ---------------------------------------------------------------------------

# Volatile fragments that differ between two instances of the *same* problem.
# Removing them lets near-identical messages collapse into one drill-down row.
_SIG_SUBS = [
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "<uuid>"),
    # Long opaque tokens only — the digit lookahead keeps this from eating
    # ordinary Java class names like WorkloadExecutionException.
    (re.compile(r"\b(?=[A-Za-z0-9+/]*\d)[A-Za-z0-9+/]{24,}={0,2}\b"), "<token>"),
    (re.compile(r"\b(?:abfss|s3a?|gs|wasbs?|dbfs|file|https?)://\S+", re.I), "<path>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?Z?", re.I), "<ts>"),
    # Quoted object names: the same permission or schema fault across 40 tables
    # is one problem to fix, not 40 rows. The real name stays in the sample text.
    (re.compile(r"'[^']{1,120}'"), "'<name>'"),
    (re.compile(r'"[^"]{1,120}"'), '"<name>"'),
    (re.compile(r"\b\d+\b"), "N"),
    (re.compile(r"\s+"), " "),
]

SIGNATURE_MAX = 200


def signature(parsed: dict) -> str:
    """
    A stable key for "the same error text", used to group failures for drill-down.

    Built from details.why when the platform provides it (already human-readable)
    and the exception message otherwise, with ids, paths, tokens, timestamps and
    numbers masked so that 500 instances of one problem collapse to one row.
    """
    text = (parsed.get("why") or parsed.get("message") or "").strip()
    if not text:
        return (parsed.get("exceptionType") or "unknown").strip()

    # A raw JSON payload is all quoted strings — masking them leaves nothing
    # readable, so key it by exception type plus a trimmed head instead.
    if text[:1] in "{[":
        etype = parsed.get("exceptionType") or "error"
        return f"{etype}: {' '.join(text.split())[:120]}"[:SIGNATURE_MAX]

    for pattern, replacement in _SIG_SUBS:
        text = pattern.sub(replacement, text)
    return text.strip()[:SIGNATURE_MAX]
