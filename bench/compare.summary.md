| benchmark | metric | None | BTree | Ordered | BTree delta | Ordered delta | p BTree (ns/op) | p Ordered (ns/op) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| BenchmarkCompare_Append/mode=* | ns/op | 25.66 | 95.38 | 26.98 | +271.8% | +5.2% | 0.000 | 0.000 |
| BenchmarkCompare_Append/mode=* | records/s | 38,975,597.50 | 10,484,961.00 | 37,061,173.00 | -73.1% | -4.9% |  |  |
| BenchmarkCompare_Append/mode=* | B/op | 0.00 | 1.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_Append/mode=* | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | ns/op | 18,695.50 | 98,400.00 | 19,695.00 | +426.3% | +5.3% | 0.000 | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | records/s | 54,772,881.50 | 10,406,535.50 | 51,993,905.00 | -81.0% | -5.1% |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | B/op | 0.00 | 38,372.00 | 4.00 | n/a | n/a |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | allocs/op | 0.00 | 68.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | ns/op | 4,679.50 | 25,220.50 | 4,906.50 | +439.0% | +4.9% | 0.000 | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | records/s | 54,705,640.00 | 10,150,518.50 | 52,177,999.50 | -81.4% | -4.6% |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | B/op | 0.00 | 9,708.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | allocs/op | 0.00 | 17.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | ns/op | 1,180.50 | 6,391.00 | 1,275.00 | +441.4% | +8.0% | 0.000 | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | records/s | 54,225,104.00 | 10,013,737.00 | 50,215,815.00 | -81.5% | -7.4% |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | B/op | 0.00 | 2,336.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | allocs/op | 0.00 | 4.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | ns/op | 151.75 | 853.05 | 166.15 | +462.1% | +9.5% | 0.000 | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | records/s | 52,706,271.50 | 9,378,147.00 | 48,147,169.50 | -82.2% | -8.7% |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | B/op | 0.00 | 396.00 | 0.00 | n/a | 0 |  |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_AsOf/mode=* | ns/op | 6.42 | 154.00 | 114.65 | +2298.9% | +1686.0% | 0.000 | 0.000 |
| BenchmarkCompare_AsOf/mode=* | records/s | 155,773,413.50 | 6,493,655.50 | 8,723,857.00 | -95.8% | -94.4% |  |  |
| BenchmarkCompare_AsOf/mode=* | B/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_AsOf/mode=* | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_Range/mode=* | ns/op | 16.45 | 2,227.50 | 1,494.50 | +13436.9% | +8982.3% | 0.000 | 0.000 |
| BenchmarkCompare_Range/mode=* | records/s | 15,558,605,373.00 | 114,905,550.00 | 171,336,052.00 | -99.3% | -98.9% |  |  |
| BenchmarkCompare_Range/mode=* | B/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_Range/mode=* | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_ReadFrom/mode=* | ns/op | 1,114.00 | 1,115.50 | 1,107.50 | +0.1% | -0.6% | 0.322 | 0.000 |
| BenchmarkCompare_ReadFrom/mode=* | records/s | 229,849,507.50 | 229,476,279.00 | 231,119,810.50 | -0.2% | +0.6% |  |  |
| BenchmarkCompare_ReadFrom/mode=* | B/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_ReadFrom/mode=* | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_SubscriptionNext/mode=* | ns/op | 1,168.00 | 1,167.00 | 1,159.00 | -0.1% | -0.8% | 0.300 | 0.000 |
| BenchmarkCompare_SubscriptionNext/mode=* | records/s | 219,176,035.50 | 219,332,769.00 | 220,841,032.50 | +0.1% | +0.8% |  |  |
| BenchmarkCompare_SubscriptionNext/mode=* | B/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
| BenchmarkCompare_SubscriptionNext/mode=* | allocs/op | 0.00 | 0.00 | 0.00 | 0 | 0 |  |  |
