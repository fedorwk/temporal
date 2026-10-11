#!/usr/bin/env python3
"""Compare TimeIndexNone vs TimeIndexBTree benchmark runs without benchstat.

Reads `go test -bench` output with sub-benchmarks named .../mode=None/... and
.../mode=BTree/..., and prints a Markdown table of medians per metric plus a
two-sided Mann-Whitney U test (normal approximation with tie correction) on
ns/op. p < 0.05 marks a statistically significant difference.
"""
import math
import re
import sys
from collections import defaultdict

LINE = re.compile(r"^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+(.*)$")


def parse(path):
    runs = defaultdict(lambda: defaultdict(list))
    for line in open(path):
        m = LINE.match(line.strip())
        if not m:
            continue
        name, rest = m.groups()
        fields = rest.split()
        for value, unit in zip(fields[0::2], fields[1::2]):
            try:
                runs[name][unit].append(float(value))
            except ValueError:
                pass
    return runs


def median(xs):
    s = sorted(xs)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2 if n else float("nan")


def mann_whitney_p(a, b):
    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return float("nan")
    combined = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    ranks = [0.0] * len(combined)
    ties = 0.0
    i = 0
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        r = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = r
        t = j - i + 1
        ties += t ** 3 - t
        i = j + 1
    r1 = sum(r for r, (_, g) in zip(ranks, combined) if g == 0)
    u1 = r1 - n1 * (n1 + 1) / 2
    n = n1 + n2
    mu = n1 * n2 / 2
    var = n1 * n2 / 12 * ((n + 1) - ties / (n * (n - 1)))
    if var <= 0:
        return 1.0
    z = (abs(u1 - mu) - 0.5) / math.sqrt(var)
    return math.erfc(max(z, 0) / math.sqrt(2))


def main(path):
    runs = parse(path)
    pairs = defaultdict(dict)
    for name, metrics in runs.items():
        if "mode=None" in name:
            pairs[name.replace("mode=None", "mode=*")]["None"] = metrics
        elif "mode=BTree" in name:
            pairs[name.replace("mode=BTree", "mode=*")]["BTree"] = metrics
    units = ["ns/op", "records/s", "B/op", "allocs/op"]
    print("| benchmark | metric | None | BTree | delta | p (ns/op) |")
    print("|---|---|---:|---:|---:|---:|")
    for key in sorted(pairs):
        p = pairs[key]
        if "None" not in p or "BTree" not in p:
            continue
        pval = mann_whitney_p(p["None"].get("ns/op", []), p["BTree"].get("ns/op", []))
        for unit in units:
            a, b = p["None"].get(unit), p["BTree"].get(unit)
            if not a or not b:
                continue
            ma, mb = median(a), median(b)
            delta = f"{(mb - ma) / ma * 100:+.1f}%" if ma else ("0" if mb == 0 else "n/a")
            ps = f"{pval:.3f}" if unit == "ns/op" else ""
            print(f"| {key} | {unit} | {ma:,.2f} | {mb:,.2f} | {delta} | {ps} |")


if __name__ == "__main__":
    main(sys.argv[1])
