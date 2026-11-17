# Validation and CI, Explained

What the validation layer, the tests and the GitHub Actions workflow do, and why.

---

## 1. Two witnesses

**Analogy:** an accountant doesn't just trust the software's total; they add the receipts with a calculator. If both agree, the number is trustworthy.

```
                     data/employees.csv
           ┌─────────────────┴─────────────────┐
           ▼                                   ▼
 ┌────────────────────┐             ┌────────────────────┐
 │ WITNESS 1: PySpark │             │ WITNESS 2: plain   │
 │ src/employee_      │             │ Python (stdlib)    │
 │ analysis.py        │             │ validation/        │
 │                    │             │ validate.py        │
 └─────────┬──────────┘             └─────────┬──────────┘
           │ 37 · 10 · 33.56 · 309116 · 8     │ 37 · 10 · 33.56 · 309116 · 8
           └─────────────────┬────────────────┘
                             ▼
                  tests/test_spark_tasks.py
                  "do both witnesses agree?"
```

The two share nothing but the input file. A bug in one (a swapped schema, `>=` instead of `>`, a missing tiebreaker) can't hide, because the other disagrees.

---

## 2. `validation/validate.py`: 20 checks, no Spark

Runs anywhere Python runs (standard library only).

**Task answers (14 checks)**: every task recomputed in plain Python, e.g.

| Task | Spark | Plain Python |
|---|---|---|
| 5 | `WHERE Age > 30` | `sum(r["Age"] > 30 for r in rows)` |
| 6 | `GROUP BY` + `AVG` | group salaries in a dict, average each list |
| 10 | self-join count | Σ (occurrences of each id)² |
| 13 | `orderBy(Age asc, Salary desc)` | `sorted(key=(Age, -Salary))` |
| 15 | `lower(...).contains("o")` | `"o" in name.lower()` |

**Reconciliation (5 checks)**: the aggregates must agree with **each other**:

```
Emp_No unique                       → the self-join can't multiply rows
Σ department counts     = 50        → no employee lost or double-counted
Σ department totals     = 309,116   → matches the company total
min ≤ average ≤ max                 → every department
exact average × count   = total     → every department
```

**Data profile (informational)**: line endings (CRLF), no newline after the last row (why `wc -l` says 50), age and salary ranges, duplicate names (`Emp_Name` is not a key), IT median vs average.

```bash
python validation/validate.py            # console report, exit code 0/1
python validation/validate.py --summary report.md
```

---

## 3. The tests

**Analogy:** before trusting a calculator to check the accountant, test the calculator with sums you already know.

```
tests/test_validate.py (8 tests, plain Python)     tests/test_spark_tasks.py (20 tests, PySpark)
├─ tiny hand-made data, checkable by eye           ├─ every task's result
├─ case-insensitive "o" (catches "Oscar")          ├─ schema maps BY POSITION (swap = silent bug)
├─ duplicate keys multiply join rows               ├─ nullable=False not enforced for CSV
├─ float bonus needs rounding                      ├─ self-join: 10 columns, ambiguous "Salary"
└─ all 20 checks pass on the real data             ├─ immutability: bonus column not in original
                                                   ├─ show() returns None
                                                   ├─ deterministic order with a tiebreaker
                                                   └─ Spark == plain Python (two witnesses)
```

Many tests encode **real mistakes made while building the project**, so they can't come back unnoticed.

The Spark tests use `pytest.importorskip("pyspark")`: on a machine without PySpark they're skipped, and the plain-Python tests still run.

---

## 4. CI: the guard at the door

**Analogy:** a security guard runs the whole checklist every time anyone pushes a change. Nobody has to remember, and nobody can skip it.

```
git push / pull request / "Run workflow" button
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ Fresh Ubuntu machine (GitHub Actions)        │
│ 1. checkout                                  │
│ 2. Java 17     ← Spark runs on the JVM       │
│ 3. Python 3.12                               │
│ 4. pip install pyspark pytest                │
│ 5. validate.py → report on the summary page  │
│ 6. pytest -v   → 28 tests                    │
│ 7. run the full analysis end to end          │
└───────────────────┬─────────────────────────┘
                    ▼
        ✅ green badge   or   ❌ red badge
```

`SPARK_LOCAL_IP=127.0.0.1` avoids hostname issues on CI machines. A fresh machine every time proves the project works anywhere, not just "on my laptop."

---

## 5. Run it locally

```bash
pip install -r requirements.txt          # needs Java 17+ for PySpark
python validation/validate.py            # 20 checks, no Spark needed
pytest -v                                # 28 tests
python src/employee_analysis.py          # all 15 tasks, printed
```

## 6. Read the results on GitHub

**Actions** tab → latest **CI** run → the **Summary** page shows the validation table; the **test** job shows each step's output.

---

## 7. Break it on purpose

| Change | What fails | Lesson |
|---|---|---|
| Swap `Salary` and `Age` in `EMPLOYEE_SCHEMA` | Task 2, 6, 9, 11, 12, 13 tests | Schemas map by position |
| `Age > 30` → `Age >= 30` | Task 5 (count changes) | Read "exceeds" carefully |
| Remove the `Department` tiebreaker in Task 14 | Order test may fail | Ties need a deterministic order |
| Remove `F.lower(...)` in Task 15 | Plain-Python "Oscar" test documents why | Case-insensitive by design |
| Delete a row from the CSV | Row count, totals, reconciliation | Reconciliation catches lost data |
