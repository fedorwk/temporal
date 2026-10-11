| benchmark | metric | None | BTree | delta | p (ns/op) |
|---|---|---:|---:|---:|---:|
| BenchmarkCompare_Append/mode=* | ns/op | 25.66 | 96.25 | +275.1% | 0.000 |
| BenchmarkCompare_Append/mode=* | records/s | 38,966,724.00 | 10,390,369.00 | -73.3% |  |
| BenchmarkCompare_Append/mode=* | B/op | 0.00 | 1.00 | n/a |  |
| BenchmarkCompare_Append/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | ns/op | 18,792.50 | 99,476.00 | +429.3% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | records/s | 54,490,064.00 | 10,293,990.50 | -81.1% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | B/op | 0.00 | 38,373.50 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=1024 | allocs/op | 0.00 | 68.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | ns/op | 4,709.00 | 25,532.00 | +442.2% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | records/s | 54,362,989.00 | 10,026,761.00 | -81.6% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | B/op | 0.00 | 9,707.50 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=256 | allocs/op | 0.00 | 17.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | ns/op | 1,187.00 | 6,521.00 | +449.4% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | records/s | 53,920,990.50 | 9,814,307.00 | -81.8% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | B/op | 0.00 | 2,336.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=64 | allocs/op | 0.00 | 4.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | ns/op | 153.25 | 869.50 | +467.4% | 0.000 |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | records/s | 52,209,065.50 | 9,200,452.00 | -82.4% |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | B/op | 0.00 | 397.00 | n/a |  |
| BenchmarkCompare_AppendBatch/mode=*/batch=8 | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AsOf/mode=* | ns/op | 6.38 | 153.60 | +2306.0% | 0.000 |
| BenchmarkCompare_AsOf/mode=* | records/s | 156,647,218.00 | 6,510,978.00 | -95.8% |  |
| BenchmarkCompare_AsOf/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_AsOf/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_Range/mode=* | ns/op | 15.86 | 2,249.00 | +14075.9% | 0.000 |
| BenchmarkCompare_Range/mode=* | records/s | 16,136,443,518.50 | 113,824,114.50 | -99.3% |  |
| BenchmarkCompare_Range/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_Range/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_ReadFrom/mode=* | ns/op | 1,132.50 | 1,131.50 | -0.1% | 0.447 |
| BenchmarkCompare_ReadFrom/mode=* | records/s | 225,962,277.00 | 226,338,923.00 | +0.2% |  |
| BenchmarkCompare_ReadFrom/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_ReadFrom/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_SubscriptionNext/mode=* | ns/op | 1,195.00 | 1,196.00 | +0.1% | 0.818 |
| BenchmarkCompare_SubscriptionNext/mode=* | records/s | 214,230,396.00 | 214,065,360.50 | -0.1% |  |
| BenchmarkCompare_SubscriptionNext/mode=* | B/op | 0.00 | 0.00 | 0 |  |
| BenchmarkCompare_SubscriptionNext/mode=* | allocs/op | 0.00 | 0.00 | 0 |  |
