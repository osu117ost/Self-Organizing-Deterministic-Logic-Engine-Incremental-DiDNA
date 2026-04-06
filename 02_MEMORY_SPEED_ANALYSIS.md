# Отчет по памяти и скорости (RU)

## Ключевые наблюдения
- Всего валидных точек: `286`.
- Пиковая скорость: `core256/match_batch` на `core:adversarial` (`desktop/1M`) с `qps=403998779.924`.
- Лучшая эффективность qps/MB (индекс): `core256/match_batch` на `core:adversarial` с `qps_per_index_mb=1409897440.816`.

# Memory/Speed Report (EN)

## Key observations
- Total valid points: `286`.
- Peak throughput: `core256/match_batch` on `core:adversarial` (`desktop/1M`) with `qps=403998779.924`.
- Best qps/MB efficiency (index): `core256/match_batch` on `core:adversarial` with `qps_per_index_mb=1409897440.816`.

## Top Throughput

| track | dataset | tier | scale | algorithm | qps | p95 ms | index MB | qps/indexMB |
|---|---|---|---|---|---:|---:|---:|---:|
| core | core:adversarial | desktop | 1M | core256/match_batch | 403998779.924 | 0.000002 | 0.287 | 1409897440.816 |
| core | core:adversarial | server | 1M | core256/match_batch | 395728506.501 | 0.000003 | 0.287 | 1381035380.054 |
| core | core:adversarial | server | 1M | core256/match_batch | 389958566.902 | 0.000003 | 0.287 | 1360899123.516 |
| core | core:adversarial | desktop | 1M | core256/match_batch | 286875722.568 | 0.000003 | 0.287 | 1001154872.689 |
| core | core:adversarial | server | 1M | core256/match_batch | 255004462.578 | 0.000004 | 0.287 | 889928774.669 |
| core | core:adversarial | desktop | 1M | core256/match_batch | 254215205.883 | 0.000004 | 0.287 | 887174382.700 |
| core | core:adversarial | edge | 1M | core256/match_batch | 252729478.366 | 0.000004 | 0.287 | 881989408.074 |
| core | core:adversarial | edge | 1M | core256/match_batch | 252495284.651 | 0.000004 | 0.287 | 881172105.802 |
| core | core:adversarial | server | 1M | core256/match_batch | 251954854.729 | 0.000004 | 0.287 | 879286083.366 |
| core | core:adversarial | edge | 1M | core256/match_batch | 251608406.740 | 0.000004 | 0.287 | 878077029.880 |
