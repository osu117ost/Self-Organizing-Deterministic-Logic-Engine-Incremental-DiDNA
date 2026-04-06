# Benchmark Zone Report (RU)

## Контур
- run_id(s): `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`
- created_utc: `2026-04-06T00:23:22Z`
- git_commit: `None`
- licensing_policy: `internal_review`

## Ключевые итоги
- `core`: ok=246, skipped=0, failed=0, mean_qps=48927437.118
- `online`: ok=3, skipped=0, failed=0, mean_qps=35909.194
- `ann`: ok=40, skipped=0, failed=0, mean_qps=14835.077

# Benchmark Zone Report (EN)

## Context
- run_id(s): `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`
- created_utc: `2026-04-06T00:23:22Z`
- git_commit: `None`
- licensing_policy: `internal_review`

## Key outcomes
- `core`: ok=246, skipped=0, failed=0, mean_qps=48927437.118
- `online`: ok=3, skipped=0, failed=0, mean_qps=35909.194
- `ann`: ok=40, skipped=0, failed=0, mean_qps=14835.077

## Top rows (sample)

| track | dataset | algorithm | tier | scale | qps | p95 ms | recall@k | min_hamming | audit_gap | status |
|---|---|---|---|---|---:|---:|---:|---:|---:|---|
| core | core:random | core256/insert | server | 1M | 938474.798 | 0.001 | — | — | — | ok |
| core | core:random | core256/predict | server | 1M | 24503894.159 | 0.000 | — | — | — | ok |
| core | core:random | core256/probe | server | 1M | 24317262.460 | 0.000 | — | — | — | ok |
| core | core:random | core256/match_batch | server | 1M | 89937358.630 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/insert | server | 1M | 1106688.801 | 0.001 | — | — | — | ok |
| core | core:clustered | core256/predict | server | 1M | 23070826.167 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/probe | server | 1M | 23177971.007 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/match_batch | server | 1M | 79989281.436 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/insert | server | 1M | 3575227.442 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/predict | server | 1M | 29678738.048 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/probe | server | 1M | 29269737.609 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/match_batch | server | 1M | 250580720.821 | 0.000 | — | — | — | ok |
| core | core:replay | core256/insert | server | 1M | 1181049.726 | 0.001 | — | — | — | ok |
| core | core:replay | core256/predict | server | 1M | 23806199.444 | 0.000 | — | — | — | ok |
| core | core:replay | core256/probe | server | 1M | 23790307.472 | 0.000 | — | — | — | ok |
| core | core:replay | core256/match_batch | server | 1M | 86868746.536 | 0.000 | — | — | — | ok |
| core | core:random | core256/insert | server | 1M | 1180607.966 | 0.001 | — | — | — | ok |
| core | core:random | core256/predict | server | 1M | 22729576.680 | 0.000 | — | — | — | ok |
| core | core:random | core256/probe | server | 1M | 23197114.465 | 0.000 | — | — | — | ok |
| core | core:random | core256/match_batch | server | 1M | 89288982.900 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/insert | server | 1M | 1221953.106 | 0.001 | — | — | — | ok |
| core | core:clustered | core256/predict | server | 1M | 26794178.375 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/probe | server | 1M | 26491943.469 | 0.000 | — | — | — | ok |
| core | core:clustered | core256/match_batch | server | 1M | 135182266.250 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/insert | server | 1M | 3552075.406 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/predict | server | 1M | 29394728.731 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/probe | server | 1M | 30077060.437 | 0.000 | — | — | — | ok |
| core | core:adversarial | core256/match_batch | server | 1M | 255004462.578 | 0.000 | — | — | — | ok |
| core | core:replay | core256/insert | server | 1M | 1189241.736 | 0.001 | — | — | — | ok |
| core | core:replay | core256/predict | server | 1M | 23593756.101 | 0.000 | — | — | — | ok |

## Reproducibility
- `results.jsonl` contains raw run-level records.
- `summary.csv` contains grouped mean metrics.
- `manifest.json` contains hardware/software fingerprints and source pack.
- `legal_review_checklist.csv` tracks publication gate.
