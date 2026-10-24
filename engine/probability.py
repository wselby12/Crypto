"""Reproducible, deliberately unvalidated empirical player-prop projections.

The bootstrap keeps each historical game's opportunities and efficiency paired.
It is an executable research model, not an assertion that football outcomes are
independent or that historical performance establishes a profitable strategy.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timezone
from statistics import fmean, pstdev
from urllib.parse import urlsplit


MODEL_ID = "empirical-bootstrap-v1"
MIN_CALIBRATION_OBSERVATIONS = 100
MAX_CALIBRATION_ERROR = 0.10
MIN_CANDIDATE_BIN_OBSERVATIONS = 20
MIN_CALIBRATION_GAMES = 30
MIN_CANDIDATE_BIN_GAMES = 10
SCRIPT_NAMES = {"early_lead", "competitive", "early_deficit"}
DISCRETE_STATS = {
    "passing_yards", "rushing_yards", "receiving_yards", "rushing_receiving_yards",
    "passing_rushing_yards", "rush_rec_yards", "receptions", "rushing_attempts",
    "passing_attempts", "passing_completions", "completions", "carries", "targets",
    "touchdowns", "passing_touchdowns", "rushing_touchdowns", "receiving_touchdowns",
}
YARD_STATS = {stat for stat in DISCRETE_STATS if "yards" in stat}


def _number(value: object, label: str, *, minimum: float | None = None,
            maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{label} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{label} must be a finite number")
    if minimum is not None and result < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    if maximum is not None and result > maximum:
        raise ValueError(f"{label} must be at most {maximum}")
    return result


def _positive(value: object, label: str, *, maximum: float | None = None) -> float:
    result = _number(value, label, maximum=maximum)
    if result <= 0:
        raise ValueError(f"{label} must be greater than zero")
    return result


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an ISO timestamp with a timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO timestamp with a timezone") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone")
    return result.astimezone(timezone.utc)


def american_to_decimal(odds: int | float) -> float:
    """Convert conventional American odds, including even money, to decimal."""
    odds = _number(odds, "american_odds")
    if -100 < odds < 100:
        raise ValueError("american_odds must be <= -100 or >= 100")
    return 1 + (odds / 100 if odds > 0 else 100 / -odds)


def implied_probability(odds: int | float) -> float:
    """Break-even probability conditional on a non-push settlement."""
    return 1 / american_to_decimal(odds)


def _wilson(successes: int, count: int) -> dict[str, float]:
    p = successes / count
    z = 1.959963984540054
    denominator = 1 + z * z / count
    center = (p + z * z / (2 * count)) / denominator
    radius = z * math.sqrt(p * (1 - p) / count + z * z / (4 * count * count)) / denominator
    return {"low": max(0.0, center - radius), "high": min(1.0, center + radius)}


def _quantile(sorted_values: list[float], p: float) -> float:
    index = (len(sorted_values) - 1) * p
    lower = math.floor(index)
    upper = math.ceil(index)
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def _settlements(values: list[float], line: float, decimal_odds: float) -> dict:
    wins = sum(value > line for value in values)
    pushes = sum(value == line for value in values)
    losses = len(values) - wins - pushes
    win_probability = wins / len(values)
    loss_probability = losses / len(values)
    return {
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "probability_win": win_probability,
        "probability_loss": loss_probability,
        "probability_push": pushes / len(values),
        "unit_expected_value": win_probability * (decimal_odds - 1) - loss_probability,
    }


def _calibration(payload: dict, market_stat: str, as_of: datetime,
                 candidate_probability: float | None = None) -> dict:
    result = {
        "qualified": False,
        "reason": "No historical calibration records were supplied.",
        "observations": 0,
        "pushes_excluded": 0,
        "brier_score": None,
        "expected_calibration_error": None,
        "minimum_observations": MIN_CALIBRATION_OBSERVATIONS,
        "maximum_calibration_error": MAX_CALIBRATION_ERROR,
        "minimum_candidate_bin_observations": MIN_CANDIDATE_BIN_OBSERVATIONS,
        "candidate_probability": candidate_probability,
        "candidate_probability_bin": None,
        "candidate_bin_observations": None,
        "unique_games": 0,
        "minimum_unique_games": MIN_CALIBRATION_GAMES,
        "candidate_bin_unique_games": None,
        "minimum_candidate_bin_unique_games": MIN_CANDIDATE_BIN_GAMES,
        "candidate_bin_equal_game_win_rate": None,
        "conservative_probability_floor": None,
        "brier_baseline": None,
        "brier_baseline_scope": "Retrospective constant outcome-rate comparator, not an independently fitted out-of-sample forecast",
        "brier_skill_score": None,
        "reliability_bins": [],
        "uncertainty_method": (
            "Provisional one-sided 95% Hoeffding support floor using equal-game-weighted "
            "non-push outcomes within the candidate probability bin. Assumes independent "
            "games, comparable held-out forecasts, and no retrospective selection. "
            "It is not an independently calibrated guarantee or total prediction interval."
        ),
        "mode": payload.get("mode", "pregame"),
        "verification": "Computed from supplied retrospective records; source contents are not independently verified.",
    }
    calibration = payload.get("calibration")
    if calibration is None:
        return result
    try:
        if not isinstance(calibration, dict):
            raise ValueError("calibration must be an object")
        if calibration.get("synthetic", False) is not False:
            raise ValueError("Synthetic or ambiguously labeled calibration cannot qualify real recommendations")
        if calibration.get("model_id") != MODEL_ID:
            raise ValueError("calibration model_id does not match the executed model")
        if calibration.get("market_stat") != market_stat:
            raise ValueError("calibration market_stat does not match the analyzed prop")
        if calibration.get("mode", "pregame") != payload.get("mode", "pregame"):
            raise ValueError("calibration mode does not match the analyzed pregame/live phase")
        training_end = _timestamp(calibration.get("training_ended_at"), "calibration.training_ended_at")
        if training_end > as_of:
            raise ValueError("calibration training period ends after as_of")
        records = calibration.get("predictions")
        if not isinstance(records, list):
            raise ValueError("calibration.predictions must be a list")
        pairs: list[tuple[float, int, str]] = []
        seen: set[tuple] = set()
        prediction_ids: set[str] = set()
        for index, record in enumerate(records):
            prefix = f"calibration.predictions[{index}]"
            if not isinstance(record, dict):
                raise ValueError(f"{prefix} must be an object")
            if record.get("synthetic", False) is not False:
                raise ValueError("Synthetic or ambiguously labeled prediction records cannot qualify real recommendations")
            predicted_at = _timestamp(record.get("predicted_at"), f"{prefix}.predicted_at")
            settled_at = _timestamp(record.get("settled_at"), f"{prefix}.settled_at")
            if not training_end <= predicted_at < settled_at <= as_of:
                raise ValueError("calibration has future leakage or invalid prediction/settlement chronology")
            probability = _number(record.get("probability"), f"{prefix}.probability", minimum=0, maximum=1)
            if "outcome" not in record:
                raise ValueError(f"{prefix}.outcome is required")
            outcome = record["outcome"]
            if outcome is not None and (type(outcome) is not int or outcome not in (0, 1)):
                raise ValueError(f"{prefix}.outcome must be 0, 1, or null for a push")
            source = record.get("source_url")
            if not isinstance(source, str):
                raise ValueError(f"{prefix}.source_url must be an HTTPS source URL")
            parsed = urlsplit(source)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
                raise ValueError(f"{prefix}.source_url must be an HTTPS source URL without credentials")
            identity = record.get("prediction_id")
            if not isinstance(identity, str) or not identity or identity in prediction_ids:
                raise ValueError("calibration prediction IDs must be nonempty, unique strings")
            prediction_ids.add(identity)
            for field in ("game_id", "player_id"):
                if not isinstance(record.get(field), str) or not record[field].strip():
                    raise ValueError(f"{prefix}.{field} must be a stable, nonempty string")
            semantic_fields = ("game_id", "player_id", "line", "side")
            if all(field in record for field in semantic_fields):
                if not all(isinstance(record[field], str) and record[field] for field in ("game_id", "player_id")):
                    raise ValueError("calibration game_id and player_id must be nonempty strings")
                record_line = _number(record["line"], f"{prefix}.line", minimum=0)
                if record["side"] != "over":
                    raise ValueError("calibration side must match the over-only model")
                fingerprint = ("market", record["game_id"], record["player_id"], market_stat, record_line, "over")
            else:
                # Without semantic market identity, simultaneous predictions
                # cannot establish that they are distinct holdout observations.
                fingerprint = ("time", predicted_at, settled_at)
            if fingerprint in seen:
                raise ValueError("calibration contains duplicate prediction events; supply distinct stable market identities")
            seen.add(fingerprint)
            if outcome is None:
                result["pushes_excluded"] += 1
            else:
                pairs.append((probability, int(outcome), record["game_id"]))
        result["observations"] = len(pairs)
        result["unique_games"] = len({game_id for _, _, game_id in pairs})
        if not pairs:
            result["reason"] = "No settled non-push calibration observations were supplied."
            return result
        result["brier_score"] = fmean((probability - outcome) ** 2 for probability, outcome, _ in pairs)
        base_rate = fmean(outcome for _, outcome, _ in pairs)
        result["brier_baseline"] = fmean((base_rate - outcome) ** 2 for _, outcome, _ in pairs)
        if result["brier_baseline"] > 0:
            result["brier_skill_score"] = 1 - result["brier_score"] / result["brier_baseline"]
        bins: list[list[tuple[float, int, str]]] = [[] for _ in range(10)]
        for probability, outcome, game_id in pairs:
            bins[min(9, int(probability * 10))].append((probability, outcome, game_id))
        error = sum(
            len(bucket) / len(pairs)
            * abs(fmean(p for p, _, _ in bucket) - fmean(outcome for _, outcome, _ in bucket))
            for bucket in bins if bucket
        )
        result["expected_calibration_error"] = error
        for index, bucket in enumerate(bins):
            by_game: dict[str, list[int]] = {}
            for _, outcome, game_id in bucket:
                by_game.setdefault(game_id, []).append(outcome)
            result["reliability_bins"].append({
                "low": index / 10, "high": (index + 1) / 10,
                "observations": len(bucket), "unique_games": len(by_game),
                "predicted_mean": fmean(p for p, _, _ in bucket) if bucket else None,
                "observed_mean": fmean(outcome for _, outcome, _ in bucket) if bucket else None,
                "equal_game_weighted_win_rate": fmean(fmean(outcomes) for outcomes in by_game.values()) if by_game else None,
            })
        if candidate_probability is not None:
            candidate_bin = min(9, int(candidate_probability * 10))
            result["candidate_probability_bin"] = {"low": candidate_bin / 10, "high": (candidate_bin + 1) / 10}
            result["candidate_bin_observations"] = len(bins[candidate_bin])
            reliability = result["reliability_bins"][candidate_bin]
            result["candidate_bin_unique_games"] = reliability["unique_games"]
            result["candidate_bin_equal_game_win_rate"] = reliability["equal_game_weighted_win_rate"]
            if reliability["unique_games"]:
                radius = math.sqrt(math.log(20) / (2 * reliability["unique_games"]))
                result["conservative_probability_floor"] = max(0.0, min(
                    candidate_probability, reliability["equal_game_weighted_win_rate"] - radius))
        if len(pairs) < MIN_CALIBRATION_OBSERVATIONS:
            result["reason"] = f"At least {MIN_CALIBRATION_OBSERVATIONS} settled non-push holdout observations are required."
        elif result["unique_games"] < MIN_CALIBRATION_GAMES:
            result["reason"] = f"At least {MIN_CALIBRATION_GAMES} distinct settled games are required; correlated props do not create independent games."
        elif error > MAX_CALIBRATION_ERROR:
            result["reason"] = "Supplied holdout records exceed the calibration-error threshold."
        elif candidate_probability is not None and result["candidate_bin_observations"] < MIN_CANDIDATE_BIN_OBSERVATIONS:
            result["reason"] = f"The candidate probability range needs at least {MIN_CANDIDATE_BIN_OBSERVATIONS} settled holdout observations."
        elif candidate_probability is not None and result["candidate_bin_unique_games"] < MIN_CANDIDATE_BIN_GAMES:
            result["reason"] = f"The candidate probability range needs at least {MIN_CANDIDATE_BIN_GAMES} distinct settled games."
        elif candidate_probability is None:
            result["reason"] = "An all-push forecast has no conditional win probability supporting an edge."
        elif result["conservative_probability_floor"] <= implied_probability(payload["market"]["american_odds"]):
            result["reason"] = "The game-weighted holdout support floor does not exceed the current sportsbook break-even probability."
        else:
            result["qualified"] = True
            result["reason"] = "Supplied chronological holdout records meet sample, distinct-game, calibration-error, and conservative support-floor gates."
        return result
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        result["reason"] = str(exc)
        return result


def analyze(payload: dict) -> dict:
    """Execute a seeded paired bootstrap and explicit workload/efficiency stress.

    ``recommended_value`` is an economic gate under the model's assumptions,
    not betting approval. Evidence, exclusions, and calibration must be enforced
    independently by the evaluator. The reported Wilson interval describes only
    Monte Carlo sampling error; it does not measure real-world model uncertainty.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    as_of = _timestamp(payload.get("as_of"), "as_of")
    mode = payload.get("mode", "pregame")
    if mode not in ("pregame", "live"):
        raise ValueError("mode must be pregame or live")
    market = payload.get("market")
    projection = payload.get("projection")
    game = payload.get("game")
    if not isinstance(market, dict) or not isinstance(projection, dict) or not isinstance(game, dict):
        raise ValueError("market, projection, and game must be objects")
    if market.get("side") != "over":
        raise ValueError("This model supports over props only")
    market_stat = market.get("stat")
    if not isinstance(market_stat, str) or not market_stat:
        raise ValueError("market.stat must be a nonempty string")
    line = _number(market.get("line"), "market.line", minimum=0)
    decimal_odds = american_to_decimal(market.get("american_odds"))
    if projection.get("model_id") != MODEL_ID:
        raise ValueError(f"projection.model_id must be {MODEL_ID}")
    draws = projection.get("simulations")
    if isinstance(draws, bool) or not isinstance(draws, int) or not 1000 <= draws <= 200_000:
        raise ValueError("projection.simulations must be an integer between 1000 and 200000")
    seed = projection.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("projection.seed must be an integer")
    kickoff = _timestamp(game.get("kickoff"), "game.kickoff")
    season = game.get("season")
    if isinstance(season, bool) or not isinstance(season, int):
        raise ValueError("game.season must be an integer")
    history = payload.get("history")
    if not isinstance(history, list) or len(history) < 5:
        raise ValueError("At least five current-season historical games are required")
    samples: list[dict] = []
    game_ids: set[str] = set()
    for index, record in enumerate(history):
        prefix = f"history[{index}]"
        if not isinstance(record, dict):
            raise ValueError(f"{prefix} must be an object")
        game_id = record.get("game_id")
        if not isinstance(game_id, str) or not game_id or game_id in game_ids:
            raise ValueError("Historical game IDs must be nonempty and unique")
        game_ids.add(game_id)
        if record.get("season") != season:
            raise ValueError("History must contain only the selected current season")
        played_at = _timestamp(record.get("played_at"), f"{prefix}.played_at")
        if played_at >= as_of or played_at >= kickoff:
            raise ValueError("History must precede both as_of and game kickoff")
        opportunities = _number(record.get("opportunities"), f"{prefix}.opportunities", minimum=0)
        production = _number(record.get("production"), f"{prefix}.production", minimum=None if market_stat in YARD_STATS else 0)
        if opportunities == 0 and production != 0:
            raise ValueError("A zero-opportunity historical game must have zero production")
        samples.append({"opportunities": opportunities, "production": production, "played_at": played_at})
    samples.sort(key=lambda row: row["played_at"], reverse=True)
    rate_samples = [sample for sample in samples if sample["opportunities"] > 0]
    if not rate_samples:
        raise ValueError("At least one historical game must have positive opportunities")
    scripts = projection.get("scripts")
    if not isinstance(scripts, list) or len(scripts) != 3:
        raise ValueError("projection.scripts must contain early_lead, competitive, and early_deficit")
    script_names: set[str] = set()
    normalized_scripts: list[dict] = []
    for index, script in enumerate(scripts):
        if not isinstance(script, dict) or not isinstance(script.get("name"), str) or script["name"] not in SCRIPT_NAMES:
            raise ValueError("Every script must have one of the three supported game-script names")
        if script["name"] in script_names:
            raise ValueError("Game-script names must be unique")
        script_names.add(script["name"])
        normalized_scripts.append({
            "name": script["name"],
            "weight": _positive(script.get("weight"), f"projection.scripts[{index}].weight", maximum=1),
            "opportunity_multiplier": _positive(script.get("opportunity_multiplier"), f"projection.scripts[{index}].opportunity_multiplier"),
            "efficiency_multiplier": _positive(script.get("efficiency_multiplier"), f"projection.scripts[{index}].efficiency_multiplier"),
        })
    if not math.isclose(math.fsum(script["weight"] for script in normalized_scripts), 1, rel_tol=0, abs_tol=1e-9):
        raise ValueError("Game-script weights must sum to one")
    sensitivity = projection.get("sensitivity")
    if not isinstance(sensitivity, dict):
        raise ValueError("projection.sensitivity must be an object")
    opportunity_stress = _positive(sensitivity.get("opportunity_multiplier"), "projection.sensitivity.opportunity_multiplier", maximum=1)
    efficiency_stress = _positive(sensitivity.get("efficiency_multiplier"), "projection.sensitivity.efficiency_multiplier", maximum=1)
    discrete = market_stat in DISCRETE_STATS
    accrued_production = 0.0
    remaining_opportunities: list[int] = []
    if mode == "live":
        live = payload.get("live")
        if not isinstance(live, dict):
            raise ValueError("Live projections require a live object")
        accrued_production = _number(live.get("production"), "live.production", minimum=None if market_stat in YARD_STATS else 0)
        if discrete and not accrued_production.is_integer():
            raise ValueError("live.production must be an integer for NFL yard/count props")
        _timestamp(live.get("observed_at"), "live.observed_at")
        remaining = live.get("remaining_opportunities")
        if not isinstance(remaining, list) or not remaining:
            raise ValueError("live.remaining_opportunities must contain remaining-workload scenario samples")
        for value in remaining:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("live.remaining_opportunities samples must be nonnegative integers")
            remaining_opportunities.append(value)
    rng = random.Random(seed)
    outcomes: list[float] = []
    stressed_outcomes: list[float] = []
    opportunities_drawn: list[float] = []
    script_counts = {script["name"]: 0 for script in normalized_scripts}
    script_outcomes: dict[str, list[float]] = {script["name"]: [] for script in normalized_scripts}
    cumulative_weights: list[float] = []
    cumulative = 0.0
    for script in normalized_scripts:
        cumulative += script["weight"]
        cumulative_weights.append(cumulative)
    for _ in range(draws):
        draw_samples = rate_samples if mode == "live" else samples
        sample = draw_samples[rng.randrange(len(draw_samples))]
        choice = rng.random()
        script_index = 0 if choice < cumulative_weights[0] else 1 if choice < cumulative_weights[1] else 2
        script = normalized_scripts[script_index]
        workload = remaining_opportunities[rng.randrange(len(remaining_opportunities))] if mode == "live" else sample["opportunities"]
        opportunities = workload * script["opportunity_multiplier"]
        efficiency = (sample["production"] / sample["opportunities"] if sample["opportunities"] else 0) * script["efficiency_multiplier"]
        remaining_production = opportunities * efficiency
        production = accrued_production + remaining_production
        stress_production = accrued_production + remaining_production * opportunity_stress * efficiency_stress
        if not math.isfinite(production) or not math.isfinite(stress_production):
            raise ValueError("Projection multipliers produce nonfinite simulation outputs")
        if discrete:
            production = float(math.floor(production + 0.5))
            stress_production = float(math.floor(stress_production + 0.5))
        outcomes.append(production)
        stressed_outcomes.append(stress_production)
        opportunities_drawn.append(opportunities)
        script_counts[script["name"]] += 1
        script_outcomes[script["name"]].append(production)
    settlement = _settlements(outcomes, line, decimal_odds)
    stressed_settlement = _settlements(stressed_outcomes, line, decimal_odds)
    win_interval = _wilson(settlement["wins"], draws)
    loss_interval = _wilson(settlement["losses"], draws)
    conservative_ev = win_interval["low"] * (decimal_odds - 1) - loss_interval["high"]
    ordered = sorted(outcomes)
    ordered_opportunities = sorted(opportunities_drawn)
    hit_rates = {}
    for window in (4, 5, 10):
        values = [sample["production"] for sample in samples[:window]]
        historical = _settlements(values, line, decimal_odds)
        nonpushes = historical["wins"] + historical["losses"]
        hit_rates[f"last_{window}"] = {
            "requested_games": window,
            "sample_games": len(values),
            "complete_window": len(values) == window,
            "wins": historical["wins"],
            "losses": historical["losses"],
            "pushes": historical["pushes"],
            "hit_rate": historical["probability_win"],
            "win_rate_excluding_pushes": historical["wins"] / nonpushes if nonpushes else None,
        }
    return {
        "simulation": {
            "executed": True,
            "draws": draws,
            "seed": seed,
            "model_id": MODEL_ID,
            "method": "Remaining-workload scenario bootstrap with historical per-opportunity efficiency and a three-script mixture; sensitivity reuses the same draws." if mode == "live" else "Paired empirical game bootstrap with an explicitly weighted three-script mixture; sensitivity reuses the same draws.",
            "mode": mode,
            "historical_games": len(samples),
            "script_draws": script_counts,
            "discrete_settlement": discrete,
            "independently_validated": False,
        },
        "probability_win": settlement["probability_win"],
        "probability_loss": settlement["probability_loss"],
        "probability_push": settlement["probability_push"],
        "probability_interval_95": win_interval,
        "interval_scope": "Monte Carlo sampling error only; excludes input, parameter, and model uncertainty.",
        "baseline": {
            "mean": fmean(outcomes),
            "standard_deviation": pstdev(outcomes),
            "p10": _quantile(ordered, 0.10),
            "p50": _quantile(ordered, 0.50),
            "p90": _quantile(ordered, 0.90),
            "mean_opportunities": fmean(opportunities_drawn),
            "opportunities_p10": _quantile(ordered_opportunities, 0.10),
            "opportunities_p50": _quantile(ordered_opportunities, 0.50),
            "opportunities_p90": _quantile(ordered_opportunities, 0.90),
            "opportunities_scope": "Remaining opportunities" if mode == "live" else "Full-game opportunities",
        },
        "scripts": {
            name: {"draws": len(values), **(_settlements(values, line, decimal_odds) if values else {})}
            for name, values in script_outcomes.items()
        },
        "stress": {
            "opportunity_multiplier": opportunity_stress,
            "efficiency_multiplier": efficiency_stress,
            **stressed_settlement,
        },
        "hit_rates": hit_rates,
        "decimal_odds": decimal_odds,
        "break_even_probability": 1 / decimal_odds,
        "break_even_scope": "Conditional on a non-push result; use unit_expected_value for unconditional evaluation.",
        "unit_expected_value": settlement["unit_expected_value"],
        "conservative_unit_expected_value": conservative_ev,
        "recommended_value": conservative_ev > 0 and stressed_settlement["unit_expected_value"] > 0,
        "value_scope": "Economic screen under supplied assumptions only; not a final recommendation or calibrated probability claim.",
        "calibration": _calibration(payload, market_stat, as_of,
            settlement["probability_win"] / (settlement["probability_win"] + settlement["probability_loss"])
            if settlement["probability_win"] + settlement["probability_loss"] > 0 else None),
        "live": {
            "executed": mode == "live",
            "production_already_recorded": accrued_production if mode == "live" else None,
            "remaining_needed": max(0.0, line - accrued_production) if mode == "live" else None,
            "remaining_needed_to_win": max(0.0, math.floor(line) + 1 - accrued_production) if mode == "live" and discrete else None,
            "method": "Accrued production plus supplied remaining-workload samples times bootstrapped historical efficiency; stress affects remaining production only." if mode == "live" else None,
            "remaining_workload_scope": "Supplied scenario assumptions; not independently verified live opportunities." if mode == "live" else None,
        },
    }
