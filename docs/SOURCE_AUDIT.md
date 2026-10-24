# User-provided source audit

Preserved as supplied. Historical claims describe the framework, not software validation.

ENGINE



Full Audit: Our NFL Betting Engine

I’m going to expose the entire operational workflow, decision rules, scoring methodology, data verification requirements, simulation process, and failure points so you can audit it.

First, an important distinction: our engine is a framework of instructions and analytical rules, not an independently running, validated betting software system.

When you ask me to run the engine, I use available data, statistical reasoning, research tools, and the rules we’ve developed. Unless I actually execute simulation code or run a backtest, I have not performed a numerical simulation.

I should never describe an analysis as having passed 46 checks, run 10,000 simulations, or achieved 90% confidence unless those things actually happened.

1. Engine architecture

Our framework evolved through several versions.

Version

Recorded scope

Audit status

Original

25 rules

Reconstructed checklist; original wording not fully recovered

Engine 2.0

38 checks

Workload, game scripts, probability, value

Expanded framework

46 checks

Referenced in our discussions

Proposed Engine 3.0

54 checks

Eight additions proposed; overlaps not fully reconciled

Strategy laboratory

500 strategies

Proposed, not actually backtested

Critical finding: The 54-check framework and 500-strategy laboratory must not be represented as completed, validated systems.

2. The complete step-by-step process

This is the intended process, with an explanation of what I actually do and what can go wrong.

Phase 1 — Identify the best games

Step 1: Retrieve the complete schedule.

Confirm game dates, kickoff times, opponents, home/away status, and current season.

Step 2: Verify current rosters.

Confirm every player’s actual team, position, and role. Do not rely on outdated roster knowledge.

Step 3: Identify the most predictable games.

Evaluate projected competitiveness, pace, defensive weaknesses, and expected offensive opportunities.

Step 4: Rank games before selecting props.

Avoid choosing a player first and then building a favorable narrative around that player.

Step 5: Identify suitable prop categories.

Our preference is overs, particularly conservative alternate lines with stable opportunities.

Required output: Ranked games, supporting evidence, and the specific prop categories worth investigating.

Phase 2 — Collect verified player data

Step 6: Retrieve current-season statistics.

Use the current season, not career averages or outdated projections.

Step 7: Examine the last 4, 5, and 10 games.

Calculate production, variance, and changes in usage.

Step 8: Calculate exact-line hit rates.

Count how often the player exceeded the specific proposed threshold.

Step 9: Examine home/away splits.

Determine whether venue materially changes the player’s opportunity or efficiency.

Step 10: Evaluate opponent-specific defensive performance.

Include yards, receptions, attempts, and success rates allowed to the relevant position.

Step 11: Examine comparable opponents.

Identify previous matchups against similar defensive schemes and personnel.

Step 12: Analyze historical misses.

Determine why the player failed similar lines: injuries, reduced snaps, coverage, game script, efficiency, or workload.

Required output: Actual numerical evidence, with sources and dates.

Phase 3 — Opportunity and workload

Step 13: Project opportunities before production.

For example:

QB rushing: designed runs + expected scrambles
WR receiving: routes × targets per route
RB rushing: projected carries × yards per carry
TE receiving: routes × target share × yards per target
Step 14: Verify snap participation.

Examine recent snap counts and trends.

Step 15: Verify rotation.

Determine whether backups or committee members are gaining playing time.

Step 16: Analyze injury-related workload restrictions.

A player being active does not establish that he has an unrestricted role.

Step 17: Review offensive-line health.

Evaluate blockers, protection changes, and expected rushing efficiency.

Step 18: Assess competing opportunities.

Consider teammates who may take targets, carries, red-zone opportunities, or snaps.

Step 19: Rate workload certainty.

Distinguish a predictable workload from one dependent on coaching decisions.

Required output: An opportunity projection with a realistic range and explicit uncertainty.

Phase 4 — Mandatory adversarial investigation

This is the phase that failed with Jayden Daniels.

Step 20: Search for coaching statements that contradict the pick.

Look for press conferences, interviews, announcements, and game-plan comments.

Step 21: Search for injury-management restrictions.

Include scrambling limitations, snap caps, pitch counts, and protective play-calling.

Step 22: Review credible beat-reporter information.

Prioritize direct reporting over speculation.

Step 23: Review relevant pregame and broadcast reporting.

Investigate announced usage plans and last-minute changes.

Step 24: Examine defensive plans designed to stop the player.

For a mobile quarterback, specifically investigate spies, containment, and pocket-control strategies.

Step 25: Identify the strongest statistical argument against the pick.

Find the most relevant historical failures and unfavorable matchup data.

Step 26: Identify plausible coaching decisions that would invalidate the projection.

For example, a team may protect a quarterback by emphasizing quick passes and limiting designed runs.

Step 27: Perform the final contradiction check.

Ask: What credible evidence would make this recommendation wrong? Have I actively searched for it?

Required output: A documented case against the pick, not just a list of reasons it might win.

Mandatory rejection rule: If a critical usage restriction remains unresolved, the prop cannot receive a high-confidence recommendation.

Phase 5 — Matchup and game environment

Step 28: Evaluate offensive and defensive efficiency, not just rankings.

Step 29: Analyze scheme matchups, including blitz rates, coverage, QB containment, and defensive fronts.

Step 30: Review weather, especially wind and precipitation.

Step 31: Examine rest, travel, short weeks, and fatigue.

Step 32: Evaluate projected pace and total possessions.

Step 33: Evaluate red-zone usage separately from general production.

Step 34: Model three game scripts: early lead, competitive game, and early deficit.

Step 35: Stress-test extreme scripts, including blowouts, early injuries, and significant personnel changes.

Required output: Explain whether the pick survives different realistic game situations.

Phase 6 — Mathematical modeling and simulations

Step 36: Build a baseline projection.

Use expected opportunities and expected efficiency to estimate a distribution of outcomes, rather than relying solely on the player’s average.

Step 37: Separate the components of production.

For receiving yards, for example:

EY=ET\times P(\text{catch})\times E\\text{yards per catch}

Where is targets. This is a simplified expectation model, not a complete probability distribution.

Step 38: Model variance.

Include uncertainty in opportunities, efficiency, playing time, and game script.

Step 39: Execute simulations when justified.

A proper Monte Carlo simulation repeatedly samples from specified statistical distributions and produces an estimated outcome frequency.

A prior October 19 analysis reported 200,000 Monte Carlo draws using assumed team scoring means of 28.5 and 22.5 points. Those were scenario assumptions, not a validated player-prop prediction model. That simulation did not establish that Daniels would exceed his rushing line.

Step 40: Calculate the probability of exceeding the exact line.

This requires an actual outcome distribution. A subjective confidence rating cannot substitute for it.

Step 41: Stress-test model assumptions.

Change projected snaps, targets, attempts, efficiency, and coaching strategy. Determine whether the conclusion remains stable.

Step 42: Assess correlated picks.

Two picks in the same game may depend on the same underlying event. Their combined probability cannot automatically be calculated by multiplying independent probabilities.

Required output: A reproducible projection with assumptions, calculations, uncertainty, and sensitivity tests. If no simulation was executed, state that plainly.

Phase 7 — Sportsbook and market analysis

Step 43: Retrieve the exact sportsbook line.

Step 44: Retrieve current odds.

Step 45: Compare opening and current lines.

Step 46: Check for relevant line movement and potential information-driven changes.

Step 47: Compare alternate thresholds, prioritizing meaningful statistical cushions.

Step 48: Calculate the implied break-even probability.

For American odds of -110:

P_{\text{break-even}}=\frac{110}{110+100}=52.38\%

Step 49: Calculate estimated expected value using a defensible probability estimate.

For a $100 stake at -110:

EV=P(\text{win})\times90.91-P(\text{lose})\times100

Step 50: Reject the recommendation if a reliable probability edge cannot be established.

Required output: Exact line, odds, estimated probability, break-even probability, and expected value—or an explicit statement that value remains unverified.

Phase 8 — Confidence scoring and approval

Our documented Engine 2.0 used these weights:

Factor

Weight

Workload and role certainty

25%

Recent usage

15%

Matchup

15%

Game script and pace

15%

Injuries and offensive line

10%

Historical performance and volatility

10%

Sportsbook price and value

10%

Total

100%

Important: These weights were heuristically chosen. They were never demonstrated to be statistically calibrated.

A rating of 9/10 was therefore a subjective safety score, not proof of a 90% chance of winning.

For final approval, the engine should require verified critical information, an adequately supported projection, explicit counterarguments, and acceptable sportsbook value.

The engine must be allowed to return NO BET. Your instructions never required me to manufacture four supposedly safe selections when none qualified.

Phase 9 — Live-game engine

Live analysis requires additional checks that cannot simply reuse pregame projections.

Step 51: Retrieve the latest official game statistics.

Step 52: Verify the current quarter, score, possession, and game clock.

Step 53: Confirm the player’s current availability and participation.

Step 54: Calculate the remaining production needed to hit the line.

Step 55: Compare that requirement with remaining opportunities, not just historical averages.

Step 56: Check for in-game coaching adjustments and announced restrictions.

Step 57: Evaluate whether game script has changed enough to invalidate the pregame projection.

Step 58: Check live odds and whether the potential return justifies the remaining risk.

Step 59: Reject stale or unverifiable information.

Required output: A timestamped assessment using verified current statistics. Without live verification, I should not claim to have completed the live engine.

Phase 10 — Results and continuous improvement

Step 60: Save the original recommendation, timestamp, threshold, and odds.

Step 61: Record the official result and whether the exact prop won.

Step 62: Track net units and ROI separately from hit rate.

Step 63: Track closing-line value.

Step 64: Classify failures by cause: workload, injury, coaching, matchup, script, statistical variance, or bad information.

Step 65: Evaluate performance separately by prop category.

Step 66: Compare strategies using out-of-sample testing, not a single successful game.

Step 67: Maintain a record of rejected recommendations and why they were rejected.

Step 68: Update the methodology only when evidence supports a change.

These 68 steps are a consolidated audit workflow, not a claim that the historical 54-check engine contained these exact 68 rules or that I executed them.

3. What actually happened versus what I claimed

Mechanism

Intended

Actual status

Current roster verification

Every player

Inconsistent

Injury and coaching reports

Mandatory

Missed critical information

Adversarial research

Every pick

Inadequate

Statistical projections

Quantitative

Sometimes qualitative

Monte Carlo simulations

Reproducible

Some scenario calculations reported; no validated prop model

Confidence ratings

Evidence-supported

Often subjective

Sportsbook value

Required

Frequently incomplete

500-strategy backtesting

Historical validation

Not executed

Results tracking

Every pick

Partial

Automatic rejection

Critical contradictions

Not consistently enforced

4. The most serious structural defect

The engine currently has no independently enforced quality gate.

That means I can describe a checklist, evaluate a few factors, and still produce a recommendation even when essential checks are incomplete.

There is no external software process automatically preventing that recommendation.

That explains the central failure with Daniels: the existence of a coaching-information rule did not guarantee that I actually checked coaching information.

5. Questions your audit should demand answers to

Audit verification checklist

0/10 verified

Can the assistant provide sources and timestamps for every critical pregame claim?

Can it demonstrate that it searched for evidence against the recommendation?

Can it show actual input data and calculations behind every probability?

Can it prove that any claimed simulations were actually executed?

Can it distinguish a subjective 9/10 rating from a calibrated 90% probability?

Can it prove that the sportsbook line and odds were verified?

Can it show why a recommendation passed despite contradictory evidence?

Can it reproduce the original recommendation without using information published afterward?

Can it produce a complete record of wins, losses, and financial returns?

Can it reject a recommendation instead of forcing a pick?

Copy audit checklist

Final audit finding

The engine has three separate problems:

Execution failure: I did not consistently perform the checks you required.
Validation failure: The scoring system and probability estimates have not been demonstrated to predict outcomes accurately.
Accountability failure: I did not consistently preserve sufficient evidence to prove which checks were completed before each recommendation.
The most important distinction is that a detailed checklist is not the same thing as an operational, tested system.

Until the checks are documented, the probability model is validated, and missing critical evidence reliably blocks recommendations, the engine should be treated as an unvalidated research framework—not a proven betting system.



Here's the deeper version, grouped by why each type breaks a model. Each has the exclusion signal you can code as a flag.

1. Injury and availability risk

Returning from injury (first 2-4 games back). Snap counts are capped and teams rarely say by how much. Flag: missed 2+ games in the last 4 weeks.
Practice trend of DNP, then limited, then limited. This is the classic "questionable" coin flip. Flag: no full practice by Friday.
Soft-tissue history in the same season. Hamstring, groin, calf, and hip flexor injuries re-aggravate and cause in-game exits. Flag: same body part listed twice in 6 weeks.
Left the previous game early. Status is unknown until Friday or later, and inactive-at-kickoff risk is high. Flag: exited game, no final designation.
Playing through a known injury. Efficiency drops (a WR with a hamstring loses speed, a QB with a non-throwing-shoulder injury loses mobility) even when he plays full snaps. Flag: any designation in the last 14 days.
Long layoff (6+ weeks). Conditioning and timing lag even after the snap restriction ends.
Chronically injured, durable-on-paper. Players with 3+ missed-game stints in 2 seasons. A clean current report hides the base rate.
QBs with a concussion or head-injury history. Protocol timing is unpredictable and can drag across multiple weeks.
Offensive linemen out or limited. A shuffled line (2+ starters out) hurts the QB, the run game, and pass-catcher volume together.
2. Role and usage instability

Committee RBs. Snap share swings week to week on game script and who has the hot hand. Flag: top back under ~60% snap share or <70% of RB touches.
Backups who inherited a role from an injury. Volume disappears when the starter returns, and you may not know the return date. Flag: role created by someone else's absence.
Rotating WR3, TE2, and slot receivers. Route participation depends on formation and matchup. Flag: route rate under ~70% or variance above a threshold.
Rookies in their first 4-6 games. There's no baseline, and coaches ramp usage unpredictably.
New acquisitions (trades and waiver claims). They need 2-3 weeks to learn the playbook. Flag: under 3 games with the team.
Role changes after a coaching or coordinator change. Past usage stops predicting future usage. Flag: new OC or play-caller in the last 4 weeks.
Goal-line and red-zone specialists. Touchdown dependence means high variance with low floor volume.
Players whose usage depends on one teammate's status. If WR2's volume swings on WR1 being out, you're pricing two injury reports at once.
Gadget and role players (returners, trick-play users). Their opportunity is tiny and non-repeatable.
3. Quarterback and offense situation

QBs facing a benching or rotation. One bad week can reshape the whole offense. Flag: backup getting reps, or the coach publicly "evaluating."
First-time starters or QBs with fewer than 4 starts. Everything downstream of him (passing volume, target distribution) is unstable.
Offenses with a QB change in the last 2 weeks. Don't use any pass-catcher's earlier target share.
Teams with a new or interim play-caller. Pass/run mix and tempo can shift overnight.
4. Game environment

Heavy favorites (about -10 or more). Starters sit late, and RBs inflate while pass-catchers lose fourth-quarter volume.
Heavy underdogs. Garbage-time volume inflates stats in ways that don't repeat. Flag: the model overrates trailing-script stats.
Weather games (wind 15+ mph, heavy rain or snow). Passing volume and kicking get suppressed.
Short week and Thursday games. Injured players are more likely to sit, and prep time is limited.
International and travel-heavy spots. Rest and routine are disrupted.
Meaningless late-season games. Resting starters is a real risk once playoff seeding is locked. Flag: seed clinched.
5. Information and data quality

Teams that hide injury info (late-week or Sunday-morning news). You can't price it in time. Flag: team historically has frequent "surprise" inactives.
Coaches who play head games with the report. Treat their questionable tags as meaningless.
Players whose status posts after your model runs. If your pipeline closes before the 90-minute inactives list, you're modeling blind.
Stale or thin tracking data. Rookies and new signees may lack the snap and route data your features need.
Small-sample stat lines. A 2-game breakout driven by touchdowns tells you nothing. Flag: under 4 games of stable usage.
6. Structural and statistical traps

Touchdown-dependent producers. Their output depends on regression-prone scoring, not volume.
Players riding unsustainable efficiency. Look for yards per route or yards per carry far above career norms with no role change.
Bye-week returners. Rest timing means status shifts are common just before and after.
Players with contract or trade drama. Holdouts, trade rumors, or "hurt vs. not-hurt" ambiguity.
Suggested hard-exclusion filter

Exclude a player if any of these hold:

Fewer than 3 games of stable snap share or route rate with the current team and offense.
Any injury designation in the last 14 days, or a missed game in the last 4 weeks.
Final status not confirmed (Friday report or later, ideally Sunday inactives).
QB change or new play-caller in the last 2 weeks.
Spread of -10 or more in either direction.
Snap-share standard deviation above your threshold over the last 4 games.
I can turn this into a
