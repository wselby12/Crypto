"""Versioned operating thresholds recorded alongside every research decision."""

import hashlib
import json
from pathlib import Path

from . import __version__
from .probability import (MAX_CALIBRATION_ERROR, MIN_CALIBRATION_OBSERVATIONS,
                          MIN_CANDIDATE_BIN_OBSERVATIONS, MIN_CALIBRATION_GAMES,
                          MIN_CANDIDATE_BIN_GAMES, MODEL_ID)
from .robustness import MIN_SCRIPT_DRAWS, MIN_WORKLOAD_RATIO

POLICY_ID = "audit-68-v2"


def snapshot():
    """Hash explicit policy values; this is provenance, not a digital signature."""
    values = {
        "id": POLICY_ID, "engine_version": __version__, "model_id": MODEL_ID,
        "calibration": {
            "minimum_nonpush_observations": MIN_CALIBRATION_OBSERVATIONS,
            "minimum_distinct_games": MIN_CALIBRATION_GAMES,
            "maximum_expected_calibration_error": MAX_CALIBRATION_ERROR,
            "minimum_candidate_bin_observations": MIN_CANDIDATE_BIN_OBSERVATIONS,
            "minimum_candidate_bin_games": MIN_CANDIDATE_BIN_GAMES,
            "support_floor_must_exceed_break_even": True,
            "support_bound": "Equal-game-weight Hoeffding, one-sided 95%, assuming independent games",
        },
        "robustness": {
            "minimum_draws_per_script": MIN_SCRIPT_DRAWS,
            "all_three_scripts_require_conservative_positive_ev": True,
            "minimum_recent_to_previous_workload_ratio": MIN_WORKLOAD_RATIO,
            "workload_comparison": "recent three vs previous three games",
        },
        "data": {"pregame_quote_seconds": 900, "live_snapshot_seconds": 60,
                 "research_hours": 48, "missing_required_evidence_blocks": True,
                 "independent_source_authentication": False},
        "exclusions": {"minimum_stable_role_games": 3, "minimum_history_games": 5,
                       "injury_designation_lookback_days": 14,
                       "missed_game_lookback_weeks": 4,
                       "qb_playcaller_change_lookback_weeks": 2,
                       "absolute_spread_excluded_at": 10,
                       "maximum_last_four_snap_share_population_stddev": 0.10,
                       "final_status_must_be_confirmed": True,
                       "unresolved_usage_restriction_blocks": True},
        "selection": {"timezone": "America/New_York", "weekday": "Sunday",
                      "morning_ends": "13:00", "early_ends": "14:00",
                      "current_quotes_and_complete_schedule_required": True,
                      "single_props_only": True},
        "threshold_status": "Provisional operating policies; not empirically optimized guarantees",
    }
    root = Path(__file__).resolve().parent
    values["implementation_sha256"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in ("probability.py", "rules.py", "evaluation.py", "robustness.py",
                     "validation.py", "slate.py", "sunday.py", "policy.py")
    }
    digest = hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {**values, "sha256": digest, "digitally_signed": False}
