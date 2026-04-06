# Влияние данных на систему (RU)

## Что лучше работает по данным
- Для `core256/insert` максимальная скорость на режиме `core:adversarial`: `qps=4130013.912`.
- Наиболее компактный режим вставки: `core:adversarial` с `bytes_per_insert=1.502`.
- Для ANN при `recall>=0.99` лучший `p95` у `hnswlib-l2-bits` на `openml:adult`: `0.012875 ms`.

# Data Impact Report (EN)

## What data regimes fit best
- For `core256/insert`, peak speed is on `core:adversarial`: `qps=4130013.912`.
- Most compact insert regime: `core:adversarial` with `bytes_per_insert=1.502`.
- For ANN at `recall>=0.99`, best `p95` is `hnswlib-l2-bits` on `openml:adult`: `0.012875 ms`.

## Aggregated Data Regime Table

| track | dataset | regime | tier | scale | mean qps | mean p95 ms | mean bytes/insert | mean recall |
|---|---|---|---|---|---:|---:|---:|---:|
| ann | openml:adult | mixed tabular (numeric + categorical) | desktop | 1M | 23887.930 | 15.946746 | 32.000 | 0.998 |
| ann | openml:adult | mixed tabular (numeric + categorical) | edge | 1M | 19542.167 | 10.255754 | 32.000 | 0.996 |
| ann | openml:adult | mixed tabular (numeric + categorical) | server | 100M | 25832.586 | 52.181959 | 32.000 | 0.990 |
| ann | openml:adult | mixed tabular (numeric + categorical) | server | 10M | 19370.696 | 21.289349 | 32.000 | 0.877 |
| ann | openml:adult | mixed tabular (numeric + categorical) | server | 1M | 21307.299 | 10.228753 | 32.000 | 0.893 |
| ann | synthetic_binary256 | binary clustered synthetic | desktop | 1M | 7571.491 | 9.268224 | 32.000 | 0.961 |
| ann | synthetic_binary256 | binary clustered synthetic | edge | 1M | 6764.483 | 10.551512 | 32.000 | 0.979 |
| ann | synthetic_binary256 | binary clustered synthetic | server | 100M | 8990.447 | 50.143778 | 32.000 | 0.939 |
| ann | synthetic_binary256 | binary clustered synthetic | server | 10M | 7439.561 | 22.314966 | 32.000 | 0.949 |
| ann | synthetic_binary256 | binary clustered synthetic | server | 1M | 7644.110 | 10.921240 | 32.000 | 0.978 |
| core | core:adversarial | prefix-collision adversarial | desktop | 1M | 88383149.805 | 0.000090 | 1.502 | 0.000 |
| core | core:adversarial | prefix-collision adversarial | edge | 1M | 78556331.732 | 0.000092 | 1.502 | 0.000 |
| core | core:adversarial | prefix-collision adversarial | server | 1M | 92930428.519 | 0.000092 | 1.502 | 0.000 |
| core | core:clustered | near-duplicate clustered keys | desktop | 1M | 43269405.948 | 0.000259 | 61.733 | 0.000 |
| core | core:clustered | near-duplicate clustered keys | edge | 1M | 41182419.026 | 0.000251 | 61.733 | 0.000 |
| core | core:clustered | near-duplicate clustered keys | server | 1M | 41105757.775 | 0.000284 | 61.733 | 0.000 |
| core | core:legacy_compat | core-legacy_compat | desktop | 1M | 93000.000 | 0.000000 | 0.000 | 0.000 |
| core | core:legacy_compat | core-legacy_compat | edge | 1M | 111000.000 | 0.000000 | 0.000 | 0.000 |
| core | core:legacy_compat | core-legacy_compat | server | 1M | 91000.000 | 0.000000 | 0.000 | 0.000 |
| core | core:random | uniform-random 256-bit | desktop | 1M | 35224856.627 | 0.000266 | 63.500 | 0.000 |
| core | core:random | uniform-random 256-bit | edge | 1M | 34499440.951 | 0.000255 | 63.500 | 0.000 |
| core | core:random | uniform-random 256-bit | server | 1M | 33629342.445 | 0.000282 | 63.500 | 0.000 |
| core | core:replay | replay/hash-stream | desktop | 1M | 35252122.368 | 0.000254 | 63.500 | 0.000 |
| core | core:replay | replay/hash-stream | edge | 1M | 35333782.000 | 0.000274 | 63.500 | 0.000 |
| core | core:replay | replay/hash-stream | server | 1M | 35086573.792 | 0.000284 | 63.500 | 0.000 |
| online | online:synthetic_drift | stream with concept drift | desktop | 1M | 34593.010 | 0.001009 | 63.502 | 0.000 |
| online | online:synthetic_drift | stream with concept drift | edge | 1M | 35093.895 | 0.000984 | 63.502 | 0.000 |
| online | online:synthetic_drift | stream with concept drift | server | 1M | 38040.678 | 0.001169 | 63.502 | 0.000 |
