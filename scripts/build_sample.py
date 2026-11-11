"""Build the warehouse from the small synthetic source used by the tests, for running dbt in CI.

    python scripts/build_sample.py data/sample      # writes data/sample/raw and data/sample/warehouse
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests.conftest import write_source  # noqa: E402
from warehouse import run  # noqa: E402

root = Path(sys.argv[1] if len(sys.argv) > 1 else "data/sample")
write_source(root / "raw")
run.main(["--raw", str(root / "raw"), "--out", str(root / "warehouse")])
