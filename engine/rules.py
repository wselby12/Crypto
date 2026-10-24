"""The 68-step audit and conservative, independently enforced evidence gates.

Citation records are supplied by the researcher. A ``documented`` record means
its provenance is complete and timely, not that software verified its contents.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import statistics
from urllib.parse import urlsplit


_PHASES = (
    (1, "pregame", (
        "Retrieve the complete schedule",
        "Verify current rosters",
        "Identify the most predictable games",
        "Rank games before selecting props",
        "Identify suitable prop categories",
    )),
    (2, "pregame", (
        "Retrieve current-season statistics",
        "Examine the last 4, 5, and 10 games",
        "Calculate exact-line hit rates",
        "Examine home/away splits",
        "Evaluate opponent-specific defensive performance",
        "Examine comparable opponents",
        "Analyze historical misses",
    )),
    (3, "pregame", (
        "Project opportunities before production",
        "Verify snap participation",
        "Verify rotation",
        "Analyze injury-related workload restrictions",
        "Review offensive-line health",
        "Assess competing opportunities",
        "Rate workload certainty",
    )),
    (4, "pregame", (
        "Search for coaching statements that contradict the pick",
        "Search for injury-management restrictions",
        "Review credible beat-reporter information",
        "Review relevant pregame and broadcast reporting",
        "Examine defensive plans designed to stop the player",
        "Identify the strongest statistical argument against the pick",
        "Identify plausible coaching decisions that invalidate the projection",
        "Perform the final contradiction check",
    )),
    (5, "pregame", (
        "Evaluate offensive and defensive efficiency",
        "Analyze scheme matchups",
        "Review weather, especially wind and precipitation",
        "Examine rest, travel, short weeks, and fatigue",
        "Evaluate projected pace and total possessions",
        "Evaluate red-zone usage separately",
        "Model early-lead, competitive, and early-deficit game scripts",
        "Stress-test extreme game scripts",
    )),
    (6, "pregame", (
        "Build a baseline projection",
        "Separate the components of production",
        "Model variance",
        "Execute reproducible simulations",
        "Calculate the probability of exceeding the exact line",
        "Stress-test model assumptions",
        "Assess correlated picks",
    )),
    (7, "pregame", (
        "Retrieve the exact sportsbook line",
        "Retrieve current odds",
        "Compare opening and current lines",
        "Check relevant line movement",
        "Compare alternate thresholds",
        "Calculate the implied break-even probability",
        "Calculate estimated expected value",
        "Reject recommendations without a reliable probability edge",
    )),
    (9, "live", (
        "Retrieve the latest official game statistics",
        "Verify quarter, score, possession, and game clock",
        "Confirm current player availability and participation",
        "Calculate remaining production needed to hit the line",
        "Compare required production with remaining opportunities",
        "Check in-game coaching adjustments and restrictions",
        "Evaluate whether the changed game script invalidates the projection",
        "Check live odds and remaining risk",
        "Reject stale or unverifiable information",
    )),
    (10, "results", (
        "Save the original recommendation, timestamp, threshold, and odds",
        "Record the official result and exact prop outcome",
        "Track net units and ROI separately from hit rate",
        "Track closing-line value",
        "Classify failures by cause",
        "Evaluate performance separately by prop category",
        "Compare strategies using out-of-sample testing",
        "Maintain rejected recommendations and rejection reasons",
        "Update methodology only when supported by evidence",
    )),
)

RULES: list[dict] = []
for _phase, _stage, _titles in _PHASES:
    for _title in _titles:
        RULES.append({"id": len(RULES) + 1, "title": _title,
                      "phase": _phase, "stage": _stage})

COMPUTED_PREGAME = frozenset({7, 8, 34, 36, 37, 38, 39, 40, 41, 48, 49, 50})
COMPUTED_LIVE = frozenset({54, 55, 58})
MARKET_RULES = frozenset({43, 44, 46})
# Historic matchups and season records can legitimately predate this week.
# Their retrieval still must be recent; all citations must predate the decision.
HISTORICAL_RULES = frozenset({1, 6, 7, 8, 9, 10, 11, 12, 25, 28, 29, 33,
                             37, 38, 42, 47})


def _time(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be an ISO timestamp with a timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp with a timezone") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


def _source_problem(source: object, rule_id: int, as_of: datetime) -> str | None:
    if not isinstance(source, dict):
        return "Every source must be an object with a URL and timestamps"
    try:
        url = urlsplit(source.get("url", ""))
        valid_url = (url.scheme == "https" and bool(url.hostname)
                     and not url.username and not url.password)
    except (ValueError, TypeError, AttributeError):
        valid_url = False
    if not valid_url:
        return "Every source needs an HTTPS URL without embedded credentials"
    try:
        published = _time(source.get("published_at"), "source.published_at")
        retrieved = _time(source.get("retrieved_at"), "source.retrieved_at")
    except ValueError as exc:
        return str(exc)
    if published > as_of or retrieved > as_of:
        return "Source publication or retrieval is after the decision timestamp"
    if retrieved < published:
        return "Source retrieval is before its publication timestamp"
    limit = (timedelta(seconds=60) if 51 <= rule_id <= 59 else
             timedelta(minutes=15) if rule_id in MARKET_RULES else
             timedelta(hours=48))
    if as_of - retrieved > limit:
        return f"Source retrieval is stale for rule {rule_id}"
    if rule_id not in HISTORICAL_RULES and as_of - published > limit:
        return f"Source publication is stale for rule {rule_id}"
    return None


def evaluate_evidence(payload: dict) -> list[dict]:
    """Return all 68 audit records; never infer successful research from absence.

    Invalid top-level schema and duplicate IDs raise ``ValueError``. Incomplete,
    stale, or malformed citations become missing records, so the caller can
    produce a useful NO BET report listing the exact outstanding work.
    Computational records remain missing until the evaluator runs the model.
    Declared contradictions are preserved even if their provenance is deficient.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    mode = payload.get("mode", "pregame")
    if not isinstance(mode, str) or mode not in {"pregame", "live"}:
        raise ValueError("mode must be pregame or live")
    as_of = _time(payload.get("as_of"), "as_of")
    evidence = payload.get("evidence", [])
    if not isinstance(evidence, list):
        raise ValueError("evidence must be a list")
    indexed: dict[int, dict] = {}
    for record in evidence:
        if not isinstance(record, dict):
            raise ValueError("every evidence record must be an object")
        rule_id = record.get("rule_id")
        if type(rule_id) is not int or not 1 <= rule_id <= 68:
            raise ValueError("evidence.rule_id must be an integer from 1 to 68")
        if rule_id in indexed:
            raise ValueError(f"duplicate evidence for rule {rule_id}")
        if (not isinstance(record.get("status"), str)
                or record["status"] not in {"supported", "contradicted"}):
            raise ValueError(f"rule {rule_id}: status must be supported or contradicted")
        indexed[rule_id] = record

    results = []
    for rule in RULES:
        item = {**rule, "status": "missing", "reason": "No documented evidence supplied"}
        rule_id = rule["id"]
        if rule["stage"] == "results":
            item.update(status="not_applicable",
                        reason="Lifecycle control: evaluated by the results ledger, not pregame research")
        elif rule["stage"] == "live" and mode == "pregame":
            item.update(status="not_applicable", reason="Live-game check is not applicable to pregame analysis")
        else:
            record = indexed.get(rule_id)
            if record is not None:
                sources = record.get("sources")
                problem = None
                if not isinstance(record.get("summary"), str) or not record["summary"].strip():
                    problem = "Evidence needs a nonempty research summary"
                elif not isinstance(sources, list) or not sources:
                    problem = "Evidence needs at least one source with publication and retrieval timestamps"
                else:
                    for source in sources:
                        problem = _source_problem(source, rule_id, as_of)
                        if problem:
                            break
                if record["status"] == "contradicted":
                    summary = record.get("summary")
                    reason = (summary.strip() if isinstance(summary, str) and summary.strip()
                              else "Contradictory evidence declared")
                    item.update(status="contradicted", reason=reason)
                    if problem:
                        item["reason"] += f"; provenance also needs correction: {problem}"
                elif problem:
                    item["reason"] = problem
                else:
                    item.update(status="documented", reason=record["summary"].strip())
            if (rule_id in COMPUTED_PREGAME or rule_id in COMPUTED_LIVE) and item["status"] != "contradicted":
                item.update(status="missing", reason="Awaiting actual computation by the engine")
        results.append(item)
    return results


def _finite_number(value: object) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def exclusions(payload: dict) -> list[str]:
    """Apply the audit's proposed hard exclusions; unknown risk never means safe."""
    if not isinstance(payload, dict):
        return ["Candidate input is missing or invalid"]
    problems: list[str] = []
    player = payload.get("player", {})
    if not isinstance(player, dict):
        return ["Player information is missing or invalid"]
    risk = player.get("risk", {})
    if not isinstance(risk, dict):
        risk = {}
    required = {
        "injury_designation_last_14_days": "Injury designation within the last 14 days",
        "missed_game_last_4_weeks": "Missed a game within the last four weeks",
        "qb_changed_last_2_weeks": "Quarterback changed within the last two weeks",
        "playcaller_changed_last_2_weeks": "Play-caller changed within the last two weeks",
        "usage_restriction_unresolved": "Critical usage restriction remains unresolved",
    }
    for key, description in required.items():
        if type(risk.get(key)) is not bool:
            problems.append(f"Unknown required risk: {key}")
        elif risk[key]:
            problems.append(description)
    if type(risk.get("final_status_confirmed")) is not bool:
        problems.append("Unknown required risk: final_status_confirmed")
    elif not risk["final_status_confirmed"]:
        problems.append("Final player availability/status is not confirmed")
    stable = player.get("stable_games")
    if type(stable) is not int or stable < 0:
        problems.append("Number of stable-role games is unknown or invalid")
    elif stable < 3:
        problems.append("Fewer than three stable-role games with the current team and offense")
    game = payload.get("game", {})
    spread = game.get("spread") if isinstance(game, dict) else None
    if not _finite_number(spread):
        problems.append("Game spread is unknown or invalid")
    elif abs(spread) >= 10:
        problems.append("Absolute game spread is at least 10 points")

    history = payload.get("history", [])
    if not isinstance(history, list) or len(history) < 4:
        problems.append("Four games of snap-share history are required")
        return problems
    try:
        ordered = sorted(history, key=lambda row: _time(row.get("played_at"), "history.played_at"))
    except (ValueError, TypeError, AttributeError):
        problems.append("Snap-share history needs valid game timestamps")
        return problems
    shares = [row.get("snap_share") for row in ordered[-4:]]
    if any(not _finite_number(value) or not 0 <= value <= 1 for value in shares):
        problems.append("Last four snap shares must be known values between zero and one")
    elif statistics.pstdev(shares) > 0.10:
        problems.append("Last-four-game snap-share population standard deviation exceeds 0.10")
    return problems
