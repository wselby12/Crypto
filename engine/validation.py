"""Validate research snapshots without inventing unavailable information."""

from datetime import datetime
import json
import math
from pathlib import Path

SUPPORTED_STATS = {
    "rushing_yards", "receiving_yards", "passing_yards", "receptions",
    "rushing_attempts", "passing_attempts", "completions",
}


def timestamp(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO 8601 timestamp with timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if result.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return result


def number(value, name, minimum=None, maximum=None):
    try:
        valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
    except (OverflowError, ValueError):
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite number")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return value


def text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def object_value(value, name):
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path):
    def invalid_constant(value):
        raise ValueError(f"Non-finite JSON number: {value}")
    return json.loads(Path(path).read_text(encoding="utf-8"),
                      object_pairs_hook=_unique_keys, parse_constant=invalid_constant)


def validate(payload):
    object_value(payload, "input")
    if payload.get("schema_version") != 1 or isinstance(payload.get("schema_version"), bool):
        raise ValueError("schema_version must be 1")
    mode = payload.get("mode", "pregame")
    if not isinstance(mode, str) or mode not in {"pregame", "live"}:
        raise ValueError("mode must be pregame or live")
    if "synthetic" in payload and not isinstance(payload["synthetic"], bool):
        raise ValueError("synthetic must be a boolean")
    cutoff = timestamp(payload.get("as_of"), "as_of")
    game = object_value(payload.get("game"), "game")
    for name in ("id", "home_team", "away_team"):
        text(game.get(name), f"game.{name}")
    if game["home_team"] == game["away_team"]:
        raise ValueError("game teams must be different")
    season = game.get("season")
    if isinstance(season, bool) or not isinstance(season, int) or not 2000 <= season <= 2100:
        raise ValueError("game.season must be an integer from 2000 to 2100")
    kickoff = timestamp(game.get("kickoff"), "game.kickoff")
    if mode == "pregame" and kickoff <= cutoff:
        raise ValueError("pregame kickoff must be after as_of; use live mode for a game in progress")
    if mode == "live" and kickoff > cutoff:
        raise ValueError("live game must have started by as_of")
    if "spread" in game and game["spread"] is not None:
        number(game["spread"], "game.spread")
    player = object_value(payload.get("player"), "player")
    for name in ("id", "name", "team", "position"):
        text(player.get(name), f"player.{name}")
    if player["team"] not in {game["home_team"], game["away_team"]}:
        raise ValueError("player.team must be a team in this game")
    if player["position"] not in ("QB", "RB", "WR", "TE"):
        raise ValueError("supported player positions: QB, RB, WR, TE")
    market = object_value(payload.get("market"), "market")
    text(market.get("sportsbook"), "market.sportsbook")
    if not isinstance(market.get("stat"), str) or market.get("stat") not in SUPPORTED_STATS:
        raise ValueError(f"market.stat must be one of: {', '.join(sorted(SUPPORTED_STATS))}")
    if market.get("side") != "over":
        raise ValueError("this engine currently supports over markets only")
    number(market.get("line"), "market.line", minimum=0)
    odds = number(market.get("american_odds"), "market.american_odds")
    if not isinstance(odds, int) or abs(odds) < 100 or not -(2**63) <= odds < 2**63:
        raise ValueError("American odds must be integers <= -100 or >= 100")
    observed = timestamp(market.get("observed_at"), "market.observed_at")
    if observed > cutoff:
        raise ValueError("market.observed_at cannot be after as_of")
    history = payload.get("history")
    if not isinstance(history, list) or len(history) < 5:
        raise ValueError("at least five current-season historical games are required")
    ids = set()
    for index, row in enumerate(history):
        prefix = f"history[{index}]"
        object_value(row, prefix)
        identity = text(row.get("game_id"), f"{prefix}.game_id")
        if identity in ids or identity == game["id"]:
            raise ValueError("history must contain unique games preceding this game")
        ids.add(identity)
        if row.get("season") != season:
            raise ValueError("history must use the selected current season")
        played = timestamp(row.get("played_at"), f"{prefix}.played_at")
        if played >= min(cutoff, kickoff):
            raise ValueError("historical games must precede as_of and kickoff")
        opportunities = number(row.get("opportunities"), f"{prefix}.opportunities", minimum=0)
        production = number(row.get("production"), f"{prefix}.production")
        if not float(production).is_integer() or not float(opportunities).is_integer():
            raise ValueError("historical NFL production and opportunity counts must be integers")
        if not market["stat"].endswith("_yards") and production < 0:
            raise ValueError("count production cannot be negative")
        if opportunities == 0 and production != 0:
            raise ValueError("zero opportunities must have zero production")
        for key in ("snap_share", "route_rate"):
            if key in row:
                number(row[key], f"{prefix}.{key}", minimum=0, maximum=1)
        if "home" in row and not isinstance(row["home"], bool):
            raise ValueError(f"{prefix}.home must be a boolean")
    object_value(payload.get("projection"), "projection")
    if not isinstance(payload.get("evidence", []), list):
        raise ValueError("evidence must be an array")
    if mode == "live":
        live = object_value(payload.get("live"), "live")
        observed = timestamp(live.get("observed_at"), "live.observed_at")
        if observed > cutoff:
            raise ValueError("live.observed_at cannot be after as_of")
        for key, maximum in (("quarter", 5), ("clock_seconds", 900)):
            value = number(live.get(key), f"live.{key}", minimum=1 if key == "quarter" else 0, maximum=maximum)
            if not float(value).is_integer():
                raise ValueError(f"live.{key} must be an integer")
        score = object_value(live.get("score"), "live.score")
        for team in ("home", "away"):
            value = number(score.get(team), f"live.score.{team}", minimum=0)
            if not float(value).is_integer():
                raise ValueError("live scores must be integers")
        if live.get("possession") not in (game["home_team"], game["away_team"]):
            raise ValueError("live.possession must identify a team in this game")
        if not isinstance(live.get("player_active"), bool):
            raise ValueError("live.player_active must be a boolean")
    return payload
