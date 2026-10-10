"""Build the whole warehouse:  python -m warehouse.run [--raw DIR] [--out DIR]

raw JSON -> staging (typed Parquet) -> star schema -> marts -> quality checks -> run_report.json
Exits with status 1 if an error-level check fails.
"""
import argparse
import json
import sys
import time
from pathlib import Path

from . import config, marts, model, quality, staging


def tree_size(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file() and not f.name.startswith((".", "_")))


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--raw", type=Path, default=config.RAW)
    p.add_argument("--out", type=Path, default=config.OUT)
    a = p.parse_args(argv)
    spark = config.quiet(config.spark())
    timings, t0 = {}, time.time()

    def step(name, fn):
        s = time.time()
        value = fn()
        timings[name] = round(time.time() - s, 1)
        print(f"{name:<10} {timings[name]:>7.1f}s", flush=True)
        return value

    stg = step("staging", lambda: staging.stage(spark, a.raw, a.out))
    tables = step("model", lambda: model.build(spark, stg, a.out))
    tables.update(step("marts", lambda: marts.build(spark, tables, a.out)))
    checks = step("quality", lambda: quality.run(stg, tables, raw=a.raw, out=a.out))

    raw_files = [f for f in a.raw.rglob("*.json")]
    report = {
        "source": {"repo": config.SOURCE_REPO, "commit": config.SOURCE_COMMIT, "files": len(raw_files),
                   "bytes": sum(f.stat().st_size for f in raw_files)},
        "seconds": {**timings, "total": round(time.time() - t0, 1)},
        "tables": {n: {"rows": df.count(), "bytes": tree_size(a.out / n)} for n, df in sorted(tables.items())},
        "warehouse_bytes": sum(tree_size(a.out / n) for n in tables),
        "staging_bytes": tree_size(a.out / "staging"),
        "checks": checks,
    }
    (a.out / "run_report.json").write_text(json.dumps(report, indent=2))

    for c in checks:
        mark = "PASS" if c["passed"] else ("WARN" if c["severity"] == "warn" else "FAIL")
        print(f"{mark}  {c['check']}: {c['failures']} ({c['detail']})")
    failed = [c["check"] for c in checks if not c["passed"] and c["severity"] == "error"]
    spark.stop()
    if failed:
        print(f"\n{len(failed)} error-level checks failed: {failed}")
        sys.exit(1)
    return report


if __name__ == "__main__":
    main()
