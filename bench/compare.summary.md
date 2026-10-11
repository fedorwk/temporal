| benchmark | metric | None | BTree | delta | p (ns/op) |
|---|---|---:|---:|---:|---:|
| BenchmarkCompare_Append/mode=* | ns/op | 25.55 | 95.94 | +275.4% | 0.000 |
| BenchmarkCompare_Append/mode=* | records/s | 39,130,654.00 | 10,422,681.00 | -73.4% |  |
| BenchmarkCompare_Append/mode=* | B/op | 0.00 | 1.00 | n/a |  |
| BenchmarkCompare_Append/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | ns/op | 21,175.00 | 103,297.50 | +387.8% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | records/s | 48,358,554.00 | 9,913,185.00 | -79.5% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | B/op | 0.00 | 38,375.50 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | allocs/op | 0.00 | 68.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | ns/op | 6,034.50 | 28,028.00 | +364.5% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | records/s | 42,425,499.50 | 9,133,804.50 | -78.5% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | B/op | 0.00 | 9,743.50 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | allocs/op | 0.00 | 17.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | ns/op | 1,551.00 | 8,540.50 | +450.6% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | records/s | 41,263,954.50 | 7,493,707.50 | -81.8% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | B/op | 0.00 | 2,349.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | allocs/op | 0.00 | 4.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | ns/op | 270.85 | 1,454.00 | +436.8% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | records/s | 29,533,140.00 | 5,501,344.00 | -81.4% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | B/op | 0.00 | 471.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AsOf/mode=* | ns/op | 6.38 | 151.95 | +2283.5% | 0.000 |
| BenchmarkCompare_AsOf/mode=* | records/s | 156,861,868.50 | 6,580,621.00 | -95.8% |  |
| BenchmarkCompare_AsOf/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AsOf/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_Range/mode=* | ns/op | 15.90 | 2,221.50 | +13871.7% | 0.000 |
| BenchmarkCompare_Range/mode=* | records/s | 16,103,155,281.00 | 115,245,301.00 | -99.3% |  |
| BenchmarkCompare_Range/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_Range/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_ReadFrom/mode=* | ns/op | 1,112.00 | 1,116.00 | +0.4% | 0.062 |
| BenchmarkCompare_ReadFrom/mode=* | records/s | 230,309,815.50 | 229,370,489.00 | -0.4% |  |
| BenchmarkCompare_ReadFrom/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_ReadFrom/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_SubscriptionNext/mode=* | ns/op | 1,193.50 | 1,192.50 | -0.1% | 0.425 |
| BenchmarkCompare_SubscriptionNext/mode=* | records/s | 214,479,383.50 | 214,671,471.50 | +0.1% |  |
| BenchmarkCompare_SubscriptionNext/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_SubscriptionNext/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
