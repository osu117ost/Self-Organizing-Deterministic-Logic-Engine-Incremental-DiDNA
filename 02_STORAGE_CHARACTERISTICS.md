# Хранение и структурные характеристики (RU)

Состояние `core256` хранится как CDG-TCA автомат с двумя битовыми строками `(f, s)` одинаковой длины `n_t`.
При конфликте по первому несовпадению `k` рост состояния равен `d = m-k-1`; сериализация выполняется в формате `CDG1`.
Оценка памяти состояния: примерно `2 * n_t` бит полезной нагрузки + служебный заголовок формата.

## Наблюдения по benchmark
- `bytes_per_insert`: min=1.502, mean=47.559, max=63.501.
- Средняя скорость `core256/insert`: 1758294.966 qps.

# Storage and Structural Characteristics (EN)

`core256` stores state as a CDG-TCA automaton with two equal-length bit strings `(f, s)`.
On first mismatch at bit `k`, growth is `d = m-k-1`; serialization is `CDG1`.
Approximate state payload is `2 * n_t` bits plus format header overhead.

## Observed in benchmark runs
- `bytes_per_insert`: min=1.502, mean=47.559, max=63.501.
- Mean `core256/insert` speed: 1758294.966 qps.
