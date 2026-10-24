"""Select Sunday morning and 1 p.m. Eastern NFL games before considering props.

Schedule content is supplied by the caller. Source timestamps and provenance are
checked here; the contents of the cited source are not fetched or verified.
Suitability scores are a transparent research heuristic, not win probabilities.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import math
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
SOURCE_MAX_AGE = timedelta(hours=48)
TARGET_WINDOWS = {
    "morning": {"start": "00:00", "end": "13:00"},
    "early": {"start": "13:00", "end": "14:00"},
}


def _time(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be an ISO timestamp with a timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp with a timezone") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return result.astimezone(timezone.utc)


def _number(value: Any, name: str, *, minimum: float | None = None,
            maximum: float | None = None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number or null")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value.strip()


def _source(value: Any, cutoff: datetime) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("schedule.source must include URL and publication/retrieval timestamps")
    url = _text(value.get("url"), "schedule.source.url")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("schedule.source.url must be an HTTPS URL without credentials")
    published = _time(value.get("published_at"), "schedule.source.published_at")
    retrieved = _time(value.get("retrieved_at"), "schedule.source.retrieved_at")
    if published > cutoff or retrieved > cutoff:
        raise ValueError("schedule source timestamps cannot be after as_of")
    if published > retrieved:
        raise ValueError("schedule source publication cannot be after retrieval")
    if cutoff - retrieved > SOURCE_MAX_AGE:
        raise ValueError("schedule source retrieval is stale (older than 48 hours)")
    return {"url": url, "published_at": published.isoformat(), "retrieved_at": retrieved.isoformat()}


def filter_and_rank(schedule: dict, as_of: str | None = None) -> dict:
    """Return upcoming games in the requested Eastern Sunday kickoff windows.

    Required metadata: integer season, cited HTTPS source with timezone-aware
    ``published_at`` and ``retrieved_at``, and a games list. A supplied
    ``complete: true`` is recorded as a claim, never independent verification.

    Game metrics are optional so a schedule without market data remains useful.
    Missing spread/total blocks game eligibility, and all absent score components
    are reported as missing rather than filled with assumed football statistics.
    """
    if not isinstance(schedule, dict):
        raise ValueError("schedule must be an object")
    season = schedule.get("season")
    if isinstance(season, bool) or not isinstance(season, int) or not 1920 <= season <= 2200:
        raise ValueError("schedule.season must be a valid integer NFL season")
    cutoff = _time(as_of, "as_of") if as_of is not None else datetime.now(timezone.utc)
    source = _source(schedule.get("source"), cutoff)
    complete = schedule.get("complete", False)
    if not isinstance(complete, bool):
        raise ValueError("schedule.complete must be a boolean")
    synthetic = schedule.get("synthetic", False)
    if not isinstance(synthetic, bool):
        raise ValueError("schedule.synthetic must be a boolean")
    today_eastern = cutoff.astimezone(EASTERN).date()
    requested_date = schedule.get("target_date")
    if requested_date is None:
        target_date = today_eastern + timedelta(days=(6 - today_eastern.weekday()) % 7)
    else:
        if not isinstance(requested_date, str):
            raise ValueError("schedule.target_date must be a YYYY-MM-DD Sunday date")
        try:
            target_date = date.fromisoformat(requested_date)
        except ValueError as exc:
            raise ValueError("schedule.target_date must be a YYYY-MM-DD Sunday date") from exc
        if requested_date != target_date.isoformat() or target_date.weekday() != 6:
            raise ValueError("schedule.target_date must be a YYYY-MM-DD Sunday date")
        if target_date < today_eastern:
            raise ValueError("schedule.target_date cannot be before the current Eastern date")
    games = schedule.get("games")
    if not isinstance(games, list):
        raise ValueError("schedule.games must be a list")

    selected: list[dict] = []
    seen: set[str] = set()
    excluded_started = 0
    excluded_windows = 0
    for index, game in enumerate(games):
        prefix = f"schedule.games[{index}]"
        if not isinstance(game, dict):
            raise ValueError(f"{prefix} must be an object")
        game_id = _text(game.get("id"), f"{prefix}.id")
        if game_id in seen:
            raise ValueError(f"duplicate game id: {game_id}")
        seen.add(game_id)
        kickoff = _time(game.get("kickoff"), f"{prefix}.kickoff")
        home = _text(game.get("home_team"), f"{prefix}.home_team")
        away = _text(game.get("away_team"), f"{prefix}.away_team")
        if home.casefold() == away.casefold():
            raise ValueError(f"{prefix} home and away teams must differ")
        spread = _number(game.get("spread"), f"{prefix}.spread")
        total = _number(game.get("total"), f"{prefix}.total", minimum=0)
        pace = _number(game.get("pace"), f"{prefix}.pace", minimum=0)
        quality = _number(game.get("data_quality"), f"{prefix}.data_quality", minimum=0, maximum=1)
        if kickoff <= cutoff:
            excluded_started += 1
            continue
        local = kickoff.astimezone(EASTERN)
        if local.date() != target_date or local.weekday() != 6 or local.hour >= 14:
            excluded_windows += 1
            continue
        window = "morning" if local.hour < 13 else "early"
        missing = [key for key, value in (("spread", spread), ("total", total),
                                         ("pace", pace), ("data_quality", quality)) if value is None]
        exclusions = []
        if spread is None:
            exclusions.append("spread missing; competitiveness and heavy-spread exclusion unknown")
        elif abs(spread) >= 10:
            exclusions.append("absolute spread is at least 10 points")
        if total is None:
            exclusions.append("total missing; scoring opportunity context unknown")

        components = {
            "competitiveness": None if spread is None else round(40 * max(0.0, 1 - abs(spread) / 10), 6),
            "total_opportunities": None if total is None else round(35 * min(total / 60, 1.0), 6),
            "pace": None if pace is None else round(15 * min(pace / 80, 1.0), 6),
            "data_quality": None if quality is None else round(10 * quality, 6),
        }
        score = round(sum(value for value in components.values() if value is not None), 6)
        selected.append({
            "id": game_id, "kickoff": kickoff.isoformat(), "kickoff_et": local.isoformat(),
            "home_team": home, "away_team": away, "spread": spread, "total": total,
            "pace": pace, "data_quality": quality, "window": window,
            "eligible": not exclusions, "exclusions": exclusions,
            "missing_metrics": missing, "suitability_score": score,
            "score_components": components,
        })

    selected.sort(key=lambda game: (not game["eligible"], -game["suitability_score"],
                                    game["kickoff"], game["id"]))
    for rank, game in enumerate(selected, 1):
        game["rank"] = rank
    windows = {}
    for window in TARGET_WINDOWS:
        count = sum(game["window"] == window for game in selected)
        windows[window] = {
            "count": count,
            "status": "fixtures_supplied" if count else (
                "none_upcoming_in_supplied_complete_schedule" if complete
                else "unknown_incomplete_schedule"
            ),
        }
    return {
        "season": season, "as_of": cutoff.isoformat(), "timezone": EASTERN.key,
        "target_date": target_date.isoformat(), "synthetic": synthetic,
        "source": source, "source_verification": "user_supplied_not_independently_verified",
        "schedule_complete": complete,
        "ranking_method": {
            "name": "uncalibrated_game_suitability_heuristic",
            "is_win_probability": False,
            "formula": "40*max(0,1-abs(spread)/10) + 35*min(total/60,1) + 15*min(pace/80,1) + 10*data_quality",
            "missing_components": "not scored; missing spread or total prevents eligibility",
            "pace_definition": "caller-supplied expected offensive plays per team",
        },
        "windows": windows, "games": selected,
        "excluded_already_started": excluded_started, "excluded_other_windows": excluded_windows,
    }
