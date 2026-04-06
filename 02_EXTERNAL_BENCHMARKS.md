# Внешние официальные benchmark-наборы (RU)

Ниже — официальные наборы для валидации памяти/скорости, сжатия логики (truth-table) и деревьев/индексов.

# External Official Benchmark Sets (EN)

The list below contains official sources for memory/speed validation, truth-table/logic compression, and tree/index workloads.

| category | benchmark | scope | url |
|---|---|---|---|
| ANN / Similarity | ANN-Benchmarks | Recall-latency-memory tradeoffs across ANN implementations | https://github.com/erikbern/ann-benchmarks |
| ANN / Similarity | ANN-Benchmarks website plots | Recall/QPS, Recall/Build time, Recall/Index size, latency percentiles | https://ann-benchmarks.com/ |
| ANN / Similarity | Big-ANN (NeurIPS track) | Large-scale practical vector search; normalized hardware, recall/QPS leaderboards | https://big-ann-benchmarks.com/neurips23.html |
| ANN / Binary | Faiss Binary Hashing Benchmark | 256-bit binary vectors; recall, wall-clock, distance computations, random accesses | https://github.com/facebookresearch/faiss/wiki/Binary-hashing-index-benchmark |
| Tree / KV serving | YCSB | Read/write/update/scan workloads with Zipfian and related distributions | https://github.com/brianfrankcooper/YCSB |
| Tree / KV serving | YCSB workload A | 50/50 read-update, Zipfian request distribution | https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloada |
| Tree / KV serving | YCSB workload F | 50/50 read-read-modify-write, Zipfian request distribution | https://raw.githubusercontent.com/brianfrankcooper/YCSB/master/workloads/workloadf |
| Tree / Storage engine | RocksDB db_bench | Official benchmark tool with fill/read/seek/mixed workloads | https://github.com/facebook/rocksdb/wiki/Benchmarking-tools |
| Tree / Sorted index | SOSD (Search on Sorted Data) | Sorted-data lookup benchmark with many datasets and baseline index structures | https://github.com/learnedsystems/SOSD |
| Truth-table / Logic compression | EPFL Combinational Benchmark Suite | Combinational logic suite for synthesis/optimization (arithmetic/control/MtM) | https://github.com/lsils/benchmarks |
| Truth-table / Logic compression | Berkeley multi-output PLA tables (table_ex) | PLA table benchmark package used in cube-hashing extraction work | https://people.eecs.berkeley.edu/~alanmi/benchmarks/table_ex/ |
| Truth-table / Logic compression | IWLS 2005 benchmark bundle | Widely used logic synthesis benchmark set (AIG/Verilog archives) | https://people.eecs.berkeley.edu/~alanmi/benchmarks/iwls2005/ |

## Как использовать эти бенчи для выбора данных
- ANN-Benchmarks: смотреть фронт `Recall/QPS` и `Recall/Index size` для выбора режима качества/памяти.
- Big-ANN: для практики использовать треки Filtered/OOD/Sparse/Streaming и фиксировать `QPS@90% recall` на нормализованном железе.
- YCSB: использовать `workloada` и `workloadf` (Zipfian) для оценки чувствительности к «горячим» ключам и RMW-паттерну.
- EPFL + table_ex + IWLS2005: использовать для truth-table/logic compression и проверки масштабируемости на arithmetic/control/MtM.

## How to use these benchmarks for data-fit decisions
- ANN-Benchmarks: use `Recall/QPS` and `Recall/Index size` frontiers for quality-memory tradeoff selection.
- Big-ANN: use Filtered/OOD/Sparse/Streaming tracks and compare `QPS@90% recall` on normalized hardware.
- YCSB: run `workloada` and `workloadf` (Zipfian) to evaluate hot-key and read-modify-write sensitivity.
- EPFL + table_ex + IWLS2005: use for truth-table/logic compression and scalability checks on arithmetic/control/MtM circuits.
