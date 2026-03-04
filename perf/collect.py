"""Reads locust CSV stats and appends a summary row to perf/history.csv."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

HISTORY_FILE = Path(__file__).parent / "history.csv"
HEADER = ["date", "requests", "failures", "rps", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"]


def collect(stats_csv: Path) -> None:
    with open(stats_csv) as f:
        for row in csv.DictReader(f):
            if row["Name"] == "Aggregated":
                stats = row
                break
        else:
            print("No Aggregated row found in stats CSV", file=sys.stderr)
            sys.exit(1)

    entry = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "requests": stats["Request Count"],
        "failures": stats["Failure Count"],
        "rps": f'{float(stats["Requests/s"]):.1f}',
        "avg_ms": f'{float(stats["Average Response Time"]):.1f}',
        "p50_ms": stats["50%"],
        "p95_ms": stats["95%"],
        "p99_ms": stats["99%"],
        "max_ms": stats["100%"],
    }

    write_header = not HISTORY_FILE.exists()
    with open(HISTORY_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        if write_header:
            w.writeheader()
        w.writerow(entry)

    print(f"  date:     {entry['date']}")
    print(f"  requests: {entry['requests']} ({entry['failures']} failures)")
    print(f"  rps:      {entry['rps']}")
    print(f"  latency:  avg={entry['avg_ms']}ms  p50={entry['p50_ms']}ms  p95={entry['p95_ms']}ms  p99={entry['p99_ms']}ms  max={entry['max_ms']}ms")


if __name__ == "__main__":
    report_dir = Path(__file__).parent / "report"
    collect(report_dir / "results_stats.csv")
