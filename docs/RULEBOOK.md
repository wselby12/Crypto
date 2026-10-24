# Engine rulebook

The supplied **Full Audit: Our NFL Betting Engine** defines the 68-step
workflow below. The catalog preserves that numbering. Its presence is not a
claim that 68 checks have executed, that a model is calibrated, or that a pick
will win. Phase 8 specifies scoring and approval but has no separately numbered
steps in the supplied audit.

## What each status means

| Status | Meaning |
| --- | --- |
| `documented` | The researcher supplied a summary and timely, complete source citations. The software has **not independently checked the source contents**. |
| `computed` | The evaluator performed the relevant calculation and records its output. This must not be set merely because a researcher says a simulation ran. |
| `missing` | Evidence is absent, incomplete, stale, malformed, or a calculation has not run. |
| `contradicted` | The input explicitly records contrary evidence. A model result must not erase it. |
| `not_applicable` | The step belongs to another workflow stage. It has not passed. |

Pregame evaluation requires steps 1–50. Live evaluation also requires steps
51–59. Steps 60–68 are lifecycle controls, not prerequisite research that can be
declared passed before a game has finished. The ledger and subsequent analysis
provide the records needed for those controls; a stored recommendation alone
does not constitute out-of-sample validation or evidence for changing a model.

## Citation contract

Each imported evidence record identifies `rule_id`, a `supported` or
`contradicted` status, a nonempty research `summary`, and at least one source:

```json
{
  "rule_id": 20,
  "status": "supported",
  "summary": "Summarize the coaching statement and the strongest contrary finding.",
  "sources": [{
    "url": "https://www.nfl.com/news/the-actual-report",
    "published_at": "2026-10-25T11:50:00Z",
    "retrieved_at": "2026-10-25T11:55:00Z"
  }]
}
```

This is a format example, not a real report. Production records need the actual
source and its publication/retrieval times. All timestamps must include their
timezone. Publication and retrieval must precede the decision's `as_of`
timestamp, and retrieval cannot precede publication. URLs must use HTTPS and
cannot contain embedded credentials. Every supplied citation is checked; a
valid citation does not hide another citation containing future information.

Evidence normally expires after 48 hours. Sportsbook-line, current-odds, and
line-movement evidence (43, 44, 46) expires after 15 minutes. Live evidence
(51–59) expires after 60 seconds. Both publication and retrieval must be timely
for current reporting. Historical schedule/statistical context (1, 6–12, 25,
28, 29, 33, 37, 38, 42, 47) may have an older publication timestamp but still
requires recent retrieval. These are conservative operational defaults, not
empirically validated predictive thresholds.

Duplicate or invalid rule IDs are schema errors. Missing or invalid provenance
produces a `missing` record. A declared contradiction remains a contradiction
even when its provenance also needs repair. Computational steps cannot be
completed by an imported attestation.

## The 68 steps

`Research` means documented source material is needed. `Calculation` means the
engine must actually execute the calculation. `Lifecycle` means the relevant
ledger record or later validation work must exist before completion can be
claimed.

| Step | Phase | Requirement | Execution |
| --- | --- | --- | --- |
| 1 | 1 | Retrieve the complete schedule, dates, kickoff times, opponents, venues, and season | Research |
| 2 | 1 | Verify current player teams, positions, and roles | Research |
| 3 | 1 | Identify predictable games using competitiveness, pace, defense, and opportunity | Research |
| 4 | 1 | Rank games before selecting individual props | Research |
| 5 | 1 | Identify suitable prop categories; prioritize stable opportunities | Research |
| 6 | 2 | Retrieve current-season statistics | Research |
| 7 | 2 | Examine the last 4, 5, and 10 games, including sample counts | Calculation |
| 8 | 2 | Calculate hit rates at the exact proposed line | Calculation |
| 9 | 2 | Examine home/away splits | Research |
| 10 | 2 | Evaluate opponent defense against the relevant position and statistic | Research |
| 11 | 2 | Compare similar schemes and personnel | Research |
| 12 | 2 | Investigate historical misses and their causes | Research |
| 13 | 3 | Project opportunities before production | Research |
| 14 | 3 | Verify recent snap participation and trend | Research |
| 15 | 3 | Verify rotations and committee changes | Research |
| 16 | 3 | Investigate injury-related workload restrictions | Research |
| 17 | 3 | Review offensive-line availability and changes | Research |
| 18 | 3 | Assess teammates competing for opportunities | Research |
| 19 | 3 | Rate workload certainty with explicit uncertainty | Research |
| 20 | 4 | Search for coaching statements against the pick | Research |
| 21 | 4 | Search for injury management, snap caps, and protective play-calling | Research |
| 22 | 4 | Review credible direct beat reporting | Research |
| 23 | 4 | Review pregame and broadcast reporting for late usage changes | Research |
| 24 | 4 | Investigate defensive containment and player-specific plans | Research |
| 25 | 4 | State the strongest statistical argument against the pick | Research |
| 26 | 4 | Identify coaching decisions that would invalidate the projection | Research |
| 27 | 4 | Perform the final contradiction check | Research |
| 28 | 5 | Evaluate offensive and defensive efficiency, not merely rankings | Research |
| 29 | 5 | Analyze coverage, blitzing, fronts, and containment | Research |
| 30 | 5 | Review wind, precipitation, and other weather | Research |
| 31 | 5 | Examine rest, travel, short weeks, and fatigue | Research |
| 32 | 5 | Evaluate pace and total possessions | Research |
| 33 | 5 | Evaluate red-zone usage separately | Research |
| 34 | 5 | Model early-lead, competitive, and early-deficit scripts | Calculation |
| 35 | 5 | Stress-test blowouts, early injuries, and personnel changes | Research |
| 36 | 6 | Build a baseline outcome-distribution projection | Calculation |
| 37 | 6 | Separate opportunities and efficiency components | Calculation |
| 38 | 6 | Model outcome variability and explicit assumptions | Calculation |
| 39 | 6 | Execute and record reproducible simulation draws | Calculation |
| 40 | 6 | Estimate the probability of exceeding the exact line | Calculation |
| 41 | 6 | Stress-test opportunity and efficiency assumptions | Calculation |
| 42 | 6 | Investigate correlated picks; do not presume independence | Research |
| 43 | 7 | Retrieve the exact sportsbook threshold | Research |
| 44 | 7 | Retrieve current odds | Research |
| 45 | 7 | Compare opening and current thresholds | Research |
| 46 | 7 | Investigate relevant line movement and new information | Research |
| 47 | 7 | Compare alternate thresholds and statistical cushions | Research |
| 48 | 7 | Calculate the odds-implied break-even probability | Calculation |
| 49 | 7 | Calculate expected value, with pushes refundable | Calculation |
| 50 | 7 | Reject a recommendation without a defensible probability edge | Calculation |
| 51 | 9 | Retrieve current official game statistics | Research |
| 52 | 9 | Verify quarter, score, possession, and game clock | Research |
| 53 | 9 | Confirm player availability and actual participation | Research |
| 54 | 9 | Calculate remaining production needed at the exact threshold | Calculation |
| 55 | 9 | Compare the requirement with remaining opportunities | Calculation |
| 56 | 9 | Investigate in-game coaching changes and restrictions | Research |
| 57 | 9 | Check whether changed game script invalidates the pregame projection | Research |
| 58 | 9 | Check live price against remaining risk and expected value | Calculation |
| 59 | 9 | Reject stale or unverifiable live information | Research |
| 60 | 10 | Preserve the original recommendation, timestamp, threshold, and price | Lifecycle |
| 61 | 10 | Record the sourced official result and exact prop outcome | Lifecycle |
| 62 | 10 | Track net units and ROI independently from hit rate | Lifecycle |
| 63 | 10 | Track closing-line value | Lifecycle |
| 64 | 10 | Classify losses by workload, injury, coaching, matchup, script, variance, or information | Lifecycle |
| 65 | 10 | Evaluate performance separately by prop category | Lifecycle |
| 66 | 10 | Compare strategies on out-of-sample results | Lifecycle |
| 67 | 10 | Preserve rejected recommendations and their reasons | Lifecycle |
| 68 | 10 | Require evidence before changing the methodology | Lifecycle |

## Hard exclusion layer

The audit's suggested exclusions are enforced independently of the heuristic
research score. Missing required risk information blocks a candidate; unknown
does not mean false.

| Input | Blocking condition |
| --- | --- |
| `player.stable_games` | Fewer than three stable-role games with the current team and offense, or an unknown/invalid count |
| `player.risk.injury_designation_last_14_days` | True or unknown |
| `player.risk.missed_game_last_4_weeks` | True or unknown |
| `player.risk.final_status_confirmed` | False or unknown |
| `player.risk.qb_changed_last_2_weeks` | True or unknown |
| `player.risk.playcaller_changed_last_2_weeks` | True or unknown |
| `player.risk.usage_restriction_unresolved` | True or unknown |
| `game.spread` | Absolute spread at least 10 points, or an unknown/invalid spread |
| Last four `history[].snap_share` values | Population standard deviation above 0.10, missing games, invalid timestamps, or shares outside 0–1 |

The final four snap-share observations are selected chronologically, not by
input-array position. The 0.10 threshold is an explicit conservative policy,
not an optimized or historically validated cutoff. Risk inputs must be actual
JSON booleans; strings such as `"false"` and numeric zero do not establish a
verified negative finding.

The longer risk discussion in the audit also calls out committees, rookies,
new acquisitions, QB instability, touchdown dependence, efficiency outliers,
weather, travel, surprise inactives, and late-season rest. These remain part of
the sourced research requirements; the presence of the narrower hard-filter
flags does not claim that every broader risk was automatically researched.

## Phase 8: research score and approval

The audit's legacy weights are workload/role certainty 25%, recent usage 15%,
matchup 15%, game script/pace 15%, injuries/offensive line 10%, historical
performance/volatility 10%, and price/value 10%. Those are heuristic research
weights. A 9/10 score is **not a 90% win probability**. The score must never
override missing evidence, a critical usage restriction, unsupported model
validation, or unacceptable price. The engine is allowed to return **NO BET**.
