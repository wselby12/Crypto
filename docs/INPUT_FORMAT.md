# Input snapshots

Use the JSON files in `examples/` as complete structural examples. They are fictional fixtures, and changing only their `synthetic` flag does not establish real evidence. All timestamps must include a timezone (`Z` or an explicit offset). Snapshots can be replayed at a historical `as_of`; reports retain both that cutoff and the actual execution time.

## Candidate fields

| Field | Required content |
| --- | --- |
| `schema_version` | `1` |
| `mode` | `pregame` or `live` |
| `as_of` | Research cutoff timestamp; all input observations must precede it |
| `synthetic` | Boolean; true blocks qualification |
| `game` | `id`, NFL `season`, `kickoff`, `home_team`, `away_team`, finite `spread` |
| `player` | `id`, `name`, `team`, `position` (QB/RB/WR/TE), `stable_games`, and `risk` |
| `market` | `sportsbook`, `stat`, `side: "over"`, `line`, integer `american_odds`, `observed_at` |
| `history` | At least five unique, preceding games from the selected season |
| `projection` | Model, draw count, seed, three weighted game scripts, sensitivity scenario |
| `evidence` | Individual rule records with substantive research summaries and source timestamps |
| `calibration` | Optional imported chronological holdout records; absence blocks qualification |
| `scores` | Optional seven-factor research score; never a probability |

Supported market stats: `rushing_yards`, `receiving_yards`, `passing_yards`, `receptions`, `rushing_attempts`, `passing_attempts`, and `completions`. History rows contain `game_id`, `season`, `played_at`, integer `opportunities`, integer `production`, `snap_share` between zero and one, and optional `route_rate`, `home`, and `opponent`. Include zero-opportunity games rather than dropping them. Negative NFL net yards are valid; negative counts are not. No future game or prior-season average can enter a current-season history.

The six mandatory `player.risk` booleans are `injury_designation_last_14_days`, `missed_game_last_4_weeks`, `final_status_confirmed`, `qb_changed_last_2_weeks`, `playcaller_changed_last_2_weeks`, and `usage_restriction_unresolved`. Unknown fields must remain unknown; never fill absent information with `false` to force approval. All must be false except `final_status_confirmed`, which must be true.

## Research evidence

```json
{
  "rule_id": 20,
  "status": "supported",
  "summary": "Document the actual coaching search, relevant statements, and their effect on expected usage here.",
  "sources": [{
    "url": "https://YOUR-RESEARCH-SOURCE/article",
    "published_at": "2026-10-24T12:00:00Z",
    "retrieved_at": "2026-10-24T12:05:00Z"
  }]
}
```

Use `contradicted` when the evidence contradicts the proposal; the engine preserves that block. Supply each rule at most once, with multiple sources inside the same record. HTTPS source URLs must not contain embedded credentials. A citation marked `documented` has passed structural, chronology, and freshness checks; no network research or content authentication is implied. Computational rules cannot be passed by supplied attestations: the engine runs their calculations itself. See the rulebook for computational IDs and lifecycle applicability.

## Projection and calibration

The current model ID is `empirical-bootstrap-v1`. Specify 1,000–200,000 simulations and an integer seed. The script names must be `early_lead`, `competitive`, and `early_deficit`, with positive weights summing to one and positive opportunity/efficiency multipliers. These are explicit assumptions requiring research support, not fitted facts. Sensitivity multipliers must be positive and at most one.

`backtest` accepts a dataset containing `as_of`, `model_id`, `market_stat`, `mode`, `training_ended_at`, and `cases`. Each case contains the original `snapshot`, integer `official_result`, `settled_at`, and `source_url`. All history precedes prediction, all predictions follow the training cutoff, and all settlements follow the prediction and kickoff and precede the dataset cutoff. Duplicate game/player/threshold cases are rejected. Future real results cannot be backtested.

The backtest output's `calibration` object can be used as a candidate's `calibration` field. It contains stable prediction identities, the held-out forecast probability conditional on a non-push outcome, official binary outcome (`null` for push), provenance, and timestamps. Supply actual held-out records, not scenarios designed to meet the numerical gate. All-push forecasts have no conditional win estimate and are excluded rather than assigned an invented probability. Synthetic cases propagate a synthetic calibration flag.

## Live mode

In addition to the pregame baseline and live rules 51–59, supply `live.observed_at`, `quarter` (1–5), `clock_seconds` (0–900 remaining in the current quarter), `score` with home/away values, `possession` identifying a team, `player_active`, integer current `production`, and an array of nonnegative integer `remaining_opportunities` scenario samples. A game must have kicked off. Live snapshots and quotes expire after 60 seconds. Live calibration must be from live forecasts.

The live model adds already accrued production to simulated remaining workload; it does not run a full-game forecast and call it live analysis. Remaining workloads are supplied scenario assumptions requiring sourced opportunity research.

## Optional research rating

`scores` contains all seven values on a 0–10 scale: `workload_role`, `recent_usage`, `matchup`, `script_pace`, `injuries_line`, `historical_volatility`, and `price_value`. The documented weights are 25%, 15%, 15%, 15%, 10%, 10%, and 10%. The resulting rating is labeled heuristic and cannot override any blocker.

## Schedule fields

Schedules contain `season`, `source` (`url`, `published_at`, `retrieved_at`), `games`, optional `complete`, optional `synthetic`, and optional Sunday `target_date` (`YYYY-MM-DD`). Game rows contain `id`, `kickoff`, `home_team`, `away_team`, plus market `spread`, `total`, and optional `pace` (expected offensive plays per team) and `data_quality` (0–1 research rating). Missing metrics remain missing. A supplied completeness claim is retained, not independently certified.
