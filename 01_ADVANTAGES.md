# Преимущества и ограничения (RU)

Ниже выводы только по измеренным данным текущего пакета.

## ANN: сравнительные итоги
- `openml:adult / desktop / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=67876.719 (hnswlib-l2-bits), лучший `p95`=0.013 ms (hnswlib-l2-bits).
- `openml:adult / edge / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=57437.778 (hnswlib-l2-bits), лучший `p95`=0.015 ms (hnswlib-l2-bits).
- `openml:adult / server / 100M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=75791.757 (hnswlib-l2-bits), лучший `p95`=0.014 ms (hnswlib-l2-bits).
- `openml:adult / server / 10M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=54929.880 (hnswlib-l2-bits), лучший `p95`=0.016 ms (hnswlib-l2-bits).
- `openml:adult / server / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=75783.733 (hnswlib-l2-bits), лучший `p95`=0.014 ms (hnswlib-l2-bits).
- `synthetic_binary256 / desktop / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=18294.206 (nmslib-hnsw-l2-bits), лучший `p95`=0.068 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / edge / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=18038.350 (nmslib-hnsw-l2-bits), лучший `p95`=0.072 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 100M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=22707.074 (nmslib-hnsw-l2-bits), лучший `p95`=0.059 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 10M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=18266.196 (nmslib-hnsw-l2-bits), лучший `p95`=0.073 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 1M`: лучший `recall@k`=1.000 (reference-exact-hamming), лучший `qps`=19004.448 (nmslib-hnsw-l2-bits), лучший `p95`=0.066 ms (nmslib-hnsw-l2-bits).

## Online-адаптация
- Средние: accuracy=0.500, F1=0.338, update_latency_ms=0.000861.

# Advantages and Limits (EN)

Statements below are limited to measured values in this package.

## ANN comparative outcomes
- `openml:adult / desktop / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=67876.719 (hnswlib-l2-bits), best `p95`=0.013 ms (hnswlib-l2-bits).
- `openml:adult / edge / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=57437.778 (hnswlib-l2-bits), best `p95`=0.015 ms (hnswlib-l2-bits).
- `openml:adult / server / 100M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=75791.757 (hnswlib-l2-bits), best `p95`=0.014 ms (hnswlib-l2-bits).
- `openml:adult / server / 10M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=54929.880 (hnswlib-l2-bits), best `p95`=0.016 ms (hnswlib-l2-bits).
- `openml:adult / server / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=75783.733 (hnswlib-l2-bits), best `p95`=0.014 ms (hnswlib-l2-bits).
- `synthetic_binary256 / desktop / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=18294.206 (nmslib-hnsw-l2-bits), best `p95`=0.068 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / edge / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=18038.350 (nmslib-hnsw-l2-bits), best `p95`=0.072 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 100M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=22707.074 (nmslib-hnsw-l2-bits), best `p95`=0.059 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 10M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=18266.196 (nmslib-hnsw-l2-bits), best `p95`=0.073 ms (nmslib-hnsw-l2-bits).
- `synthetic_binary256 / server / 1M`: best `recall@k`=1.000 (reference-exact-hamming), best `qps`=19004.448 (nmslib-hnsw-l2-bits), best `p95`=0.066 ms (nmslib-hnsw-l2-bits).

## Online adaptation
- Means: accuracy=0.500, F1=0.338, update_latency_ms=0.000861.
