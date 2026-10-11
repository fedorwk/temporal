#!/usr/bin/env python3
"""Compare TimeIndexMode benchmark runs without benchstat.

Reads `go test -bench` output with sub-benchmarks named .../mode=<Mode>/...
(None, BTree, Ordered) and prints a Markdown table of medians per metric,
each mode's delta against None, and a two-sided Mann-Whitney U test (normal
approximation with tie correction) on ns/op against None. p < 0.05 marks a
statistically significant difference.
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


MODES = ["None", "BTree", "Ordered"]
MODE = re.compile(r"mode=(\w+)")


def main(path):
    runs = parse(path)
    groups = defaultdict(dict)
    for name, metrics in runs.items():
        m = MODE.search(name)
        if m and m.group(1) in MODES:
            groups[MODE.sub("mode=*", name)][m.group(1)] = metrics
    others = [m for m in MODES[1:] if any(m in g for g in groups.values())]
    units = ["ns/op", "records/s", "B/op", "allocs/op"]
    head = ["benchmark", "metric", "None"] + others
    head += [f"{m} delta" for m in others] + [f"p {m} (ns/op)" for m in others]
    print("| " + " | ".join(head) + " |")
    print("|---|---|" + "---:|" * (len(head) - 2))
    for key in sorted(groups):
        g = groups[key]
        if "None" not in g:
            continue
        pvals = {m: mann_whitney_p(g["None"].get("ns/op", []), g[m].get("ns/op", [])) for m in others if m in g}
        for unit in units:
            base = g["None"].get(unit)
            if not base:
                continue
            mb = median(base)
            cells, deltas, ps = [], [], []
            for m in others:
                xs = g.get(m, {}).get(unit)
                if not xs:
                    cells.append("")
                    deltas.append("")
                    ps.append("")
                    continue
                mx = median(xs)
                cells.append(f"{mx:,.2f}")
                deltas.append(f"{(mx - mb) / mb * 100:+.1f}%" if mb else ("0" if mx == 0 else "n/a"))
                ps.append(f"{pvals[m]:.3f}" if unit == "ns/op" else "")
            row = [key, unit, f"{mb:,.2f}"] + cells + deltas + ps
            print("| " + " | ".join(row) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
