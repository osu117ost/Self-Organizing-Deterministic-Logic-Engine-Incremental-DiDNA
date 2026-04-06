# Спецификация Benchmark Zone (RU)

## Идентификация
- run_ids: `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`
- generated_utc: `2026-04-06T00:23:22Z`

## CLI-контракт
- `bench run --config bench_zone.yaml --track {core,ann,online} --tier {server,desktop,edge} --scale {1M,10M,100M}`
- `bench report --input <run_dir...> --output <dossier_dir> --lang {ru,en,both} --format {md,pdf,csv,json,xlsx,docx,all}`

## Формулы метрик
- `error_bits_min_hamming = min(popcount(query XOR candidate))`
- `error_bits_prefix_gap = 256 - matched_bits`
- `hamming_audit_gap = |pred_min_hamming - exact_min_hamming|` (по выборочным brute-force окнам)
- `recall_at_k` (tie-aware): доля кандидатов с расстоянием `<=` порогу k-го истинного соседа
- `recall_at_k_id_overlap`: классический overlap по id (диагностический)

## Схема результатов
- Базовые поля: `dataset_id, tier, scale, algorithm, build_time_s, qps, lat_p50_ms, lat_p95_ms, lat_p99_ms, rss_mb, index_bytes, bytes_per_insert, recall_at_k, update_latency_ms, energy_proxy, seed, run_id`
- Ошибки/сходство: `error_bits_min_hamming, error_bits_prefix_gap, hamming_audit_gap`
- Online KPI: `online_accuracy, online_f1, adaptation_lag_steps`
- Доп. отчеты: `dossier_memory_speed_*`, `dossier_data_impact_*`, `dossier_storage_characteristics_*`, `dossier_external_benchmarks_*`

## Официальный source pack
- https://mlcommons.org/benchmarks/
- https://docs.mlcommons.org/inference/index_gh/
- https://github.com/erikbern/ann-benchmarks
- https://ann-benchmarks.com/
- https://github.com/harsha-simhadri/big-ann-benchmarks
- https://big-ann-benchmarks.com/neurips23.html
- https://github.com/facebookresearch/faiss/wiki/Binary-hashing-index-benchmark
- https://github.com/facebookresearch/faiss
- https://github.com/nmslib/hnswlib
- https://github.com/nmslib/nmslib
- https://github.com/learnedsystems/SOSD
- https://github.com/brianfrankcooper/YCSB
- https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloada
- https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloadf
- https://github.com/facebook/rocksdb/wiki/Benchmarking-tools
- https://github.com/google/benchmark
- https://github.com/eembc/coremark
- https://github.com/lsils/benchmarks
- https://people.eecs.berkeley.edu/~alanmi/benchmarks/table_ex/
- https://people.eecs.berkeley.edu/~alanmi/benchmarks/iwls2005/
- https://moa.cms.waikato.ac.nz/
- https://riverml.xyz/latest/
- https://www.openml.org/benchmark
- https://archive.ics.uci.edu/

# Benchmark Zone Specification (EN)

## Identification
- run_ids: `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`
- generated_utc: `2026-04-06T00:23:22Z`

## CLI contract
- `bench run --config bench_zone.yaml --track {core,ann,online} --tier {server,desktop,edge} --scale {1M,10M,100M}`
- `bench report --input <run_dir...> --output <dossier_dir> --lang {ru,en,both} --format {md,pdf,csv,json,xlsx,docx,all}`

## Metric formulas
- `error_bits_min_hamming = min(popcount(query XOR candidate))`
- `error_bits_prefix_gap = 256 - matched_bits`
- `hamming_audit_gap = |pred_min_hamming - exact_min_hamming|` (sampled brute-force windows)
- `recall_at_k` (tie-aware): fraction of retrieved candidates with distance `<=` true k-th neighbor distance
- `recall_at_k_id_overlap`: classic id-overlap recall (diagnostic)

## Result schema
- Core fields: `dataset_id, tier, scale, algorithm, build_time_s, qps, lat_p50_ms, lat_p95_ms, lat_p99_ms, rss_mb, index_bytes, bytes_per_insert, recall_at_k, update_latency_ms, energy_proxy, seed, run_id`
- Error/similarity: `error_bits_min_hamming, error_bits_prefix_gap, hamming_audit_gap`
- Online KPIs: `online_accuracy, online_f1, adaptation_lag_steps`
- Additional reports: `dossier_memory_speed_*`, `dossier_data_impact_*`, `dossier_storage_characteristics_*`, `dossier_external_benchmarks_*`

## Official source pack
- https://mlcommons.org/benchmarks/
- https://docs.mlcommons.org/inference/index_gh/
- https://github.com/erikbern/ann-benchmarks
- https://ann-benchmarks.com/
- https://github.com/harsha-simhadri/big-ann-benchmarks
- https://big-ann-benchmarks.com/neurips23.html
- https://github.com/facebookresearch/faiss/wiki/Binary-hashing-index-benchmark
- https://github.com/facebookresearch/faiss
- https://github.com/nmslib/hnswlib
- https://github.com/nmslib/nmslib
- https://github.com/learnedsystems/SOSD
- https://github.com/brianfrankcooper/YCSB
- https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloada
- https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloadf
- https://github.com/facebook/rocksdb/wiki/Benchmarking-tools
- https://github.com/google/benchmark
- https://github.com/eembc/coremark
- https://github.com/lsils/benchmarks
- https://people.eecs.berkeley.edu/~alanmi/benchmarks/table_ex/
- https://people.eecs.berkeley.edu/~alanmi/benchmarks/iwls2005/
- https://moa.cms.waikato.ac.nz/
- https://riverml.xyz/latest/
- https://www.openml.org/benchmark
- https://archive.ics.uci.edu/
