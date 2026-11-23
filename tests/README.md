# Pipeline tests (pytest)

Unit tests for the Traffic-Tickets-SP medallion pipeline (bronze / silver / gold),
built on the pure transformation functions of `src/`.

## Layout

- `conftest.py` — puts `src/` on `sys.path` and provides the shared `spark` fixture
- `test_bronze.py` — column sanitization, strict paid-fines schema, CSV reading
- `test_silver.py` — silver transformations: null handling, snapshot dedup, grain aggregation, sequential IDs, date building, gold union
- `test_gold.py` — gold SQL business rules (executed against a toy `gold` temp view where possible, pinned by literals when they join real Delta tables) and the parquet export

## Running locally

```bash
pip install pyspark pytest pandas pyarrow
pytest tests -v
```

## Running on Databricks

In a notebook cell on a Spark-enabled compute (pytest ships with DBR; install with `%pip install pytest` if missing).
Some computes deny driver writes to `/Workspace` (including Python's `__pycache__`), so disable bytecode and run from a `/tmp` copy:

```python
import sys, os, shutil

sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# conftest locates src/ via MULTAS_SRC (env var or repo .env), the folder next to
# tests/, or the repo under your home dir — set this only for non-standard layouts
os.environ.setdefault("MULTAS_SRC", "/Workspace/Users/<you>/Traffic-Tickets-SP-Databricks/src")

src_tests = "/Workspace/Users/<you>/Traffic-Tickets-SP-Databricks/tests"
tmp_tests = "/tmp/tt_tests"
shutil.rmtree(tmp_tests, ignore_errors=True)
shutil.copytree(src_tests, tmp_tests)

import pytest
pytest.main([tmp_tests, "-v", "--tb=short"])
```

If you run `pytest.main` twice in the same process, evict cached test modules first
(`for m in list(sys.modules): if m.startswith(("test_", "conftest")): del sys.modules[m]`) —
pytest reuses `sys.modules` entries between in-process runs.

## Design notes

- Tests never write to the real Delta tables or the exported parquet: gold SQL tests use a throwaway `gold` temp view and the export test redirects `PARQUET_SEVERITY_MUNICIPALITIES` to pytest's `tmp_path`.
- The only read of real source data is the RENAINF reference CSV (`tabela-codigo-infracoes-renainf.csv`, 258 rows), shipped with the repo.
- The SQL metrics that join `municipios` / `infracoes_codes` cannot run against toy data (they reference real Delta tables), so their business rules are asserted as pinned literals instead.
