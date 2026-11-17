"""
Independent validation layer: recompute every task answer with plain Python.

No Spark involved, only the standard library. If Spark and this script agree,
the results are trustworthy: two independent methods, one answer.

It also reconciles the aggregates against each other (average x count = total,
department counts add up to the row count) and profiles the input file.

Usage:
    python validation/validate.py
    python validation/validate.py --data data/employees.csv --summary report.md

Exit code 0 = all checks passed, 1 = at least one failed.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA = Path(__file__).resolve().parent.parent / "data" / "employees.csv"
EXPECTED_COLUMNS = ["Emp_No", "Emp_Name", "Salary", "Age", "Department"]

# Expected results, as returned by PySpark in the IBM Skills Network lab.
EXPECTED = {
    "rows": 50,
    "older_than_30": 37,
    "it_employees": 10,
    "distinct_ages": 14,
    "max_salary_age_39": 24000,
    "self_join_rows": 50,
    "avg_age": 33.56,
    "total_salary": 309116,
    "names_with_o": 8,
    "avg_salary": {"IT": 7400.0, "Marketing": 6633.33, "HR": 5837.5, "Finance": 5730.8, "Sales": 5492.92},
    "total_by_dept": {"Sales": 71408, "IT": 74000, "Marketing": 59700, "Finance": 57308, "HR": 46700},
    "count_by_dept": {"Sales": 13, "Finance": 10, "IT": 10, "Marketing": 9, "HR": 8},
}


@dataclass
class Check:
    section: str
    name: str
    expected: object
    actual: object

    @property
    def passed(self) -> bool:
        return self.expected == self.actual


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def load(path: Path) -> list[dict]:
    """Read the CSV and cast the numeric columns, like the Spark schema does."""
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        for field in ("Emp_No", "Salary", "Age"):
            r[field] = int(r[field])
    return rows


def header(path: Path) -> list[str]:
    with path.open(encoding="utf-8", newline="") as fh:
        return next(csv.reader(fh))


# --------------------------------------------------------------------------- #
# Task answers in plain Python
# --------------------------------------------------------------------------- #
def by_department(rows: list[dict]) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        groups[r["Department"]].append(r["Salary"])
    return dict(groups)


def avg_salary_by_department(rows):           # Task 6
    return {d: round(sum(s) / len(s), 2) for d, s in by_department(rows).items()}


def total_salary_by_department(rows):         # Task 12
    return {d: sum(s) for d, s in by_department(rows).items()}


def count_by_department(rows):                # Task 14
    return {d: len(s) for d, s in by_department(rows).items()}


def max_salary_by_age(rows):                  # Task 9
    result: dict[int, int] = {}
    for r in rows:
        result[r["Age"]] = max(result.get(r["Age"], r["Salary"]), r["Salary"])
    return result


def self_join_count(rows):                    # Task 10: every id matched with every equal id
    counts = Counter(r["Emp_No"] for r in rows)
    return sum(n * n for n in counts.values())


def names_with_o(rows):                       # Task 15 (case-insensitive)
    return [r["Emp_Name"] for r in rows if "o" in r["Emp_Name"].lower()]


def sort_age_asc_salary_desc(rows):           # Task 13 reference order
    return sorted(rows, key=lambda r: (r["Age"], -r["Salary"]))


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def run_checks(path: Path) -> tuple[list[Check], dict]:
    rows = load(path)
    totals = total_salary_by_department(rows)
    counts = count_by_department(rows)
    averages = avg_salary_by_department(rows)
    max_by_age = max_salary_by_age(rows)

    checks = [
        Check("Load", "Task 1/2 - header", EXPECTED_COLUMNS, header(path)),
        Check("Load", "Task 1/2 - rows loaded", EXPECTED["rows"], len(rows)),
        Check("Tasks", "Task 5 - employees older than 30", EXPECTED["older_than_30"], sum(r["Age"] > 30 for r in rows)),
        Check("Tasks", "Task 6 - average salary by department", EXPECTED["avg_salary"], averages),
        Check("Tasks", "Task 7 - IT employees", EXPECTED["it_employees"], sum(r["Department"] == "IT" for r in rows)),
        Check("Tasks", "Task 8 - Donald's salary after bonus", 2860.0, round(2600 * 1.10, 2)),
        Check("Tasks", "Task 9 - number of age groups", EXPECTED["distinct_ages"], len(max_by_age)),
        Check("Tasks", "Task 9 - max salary at age 39", EXPECTED["max_salary_age_39"], max_by_age[39]),
        Check("Tasks", "Task 10 - self-join rows", EXPECTED["self_join_rows"], self_join_count(rows)),
        Check("Tasks", "Task 11 - average age", EXPECTED["avg_age"], round(statistics.mean(r["Age"] for r in rows), 2)),
        Check("Tasks", "Task 12 - total salary by department", EXPECTED["total_by_dept"], totals),
        Check("Tasks", "Task 13 - first row after sorting", ("Renske", 26, 3600),
              tuple(sort_age_asc_salary_desc(rows)[0][k] for k in ("Emp_Name", "Age", "Salary"))),
        Check("Tasks", "Task 13 - salaries inside the age-39 group", [24000, 9000, 8200, 6000, 4800],
              [r["Salary"] for r in sort_age_asc_salary_desc(rows) if r["Age"] == 39]),
        Check("Tasks", "Task 14 - employees per department", EXPECTED["count_by_dept"], counts),
        Check("Tasks", "Task 15 - names containing 'o'", EXPECTED["names_with_o"], len(names_with_o(rows))),
        # Reconciliation: the aggregates must agree with each other
        Check("Reconciliation", "Emp_No is unique (join cannot multiply rows)", len(rows), len({r["Emp_No"] for r in rows})),
        Check("Reconciliation", "Department counts add up to all rows", len(rows), sum(counts.values())),
        Check("Reconciliation", "Department totals add up to company total", EXPECTED["total_salary"], sum(totals.values())),
        Check(
            "Reconciliation",
            "Each average lies between its min and max",
            True,
            all(min(s) <= averages[d] <= max(s) for d, s in by_department(rows).items()),
        ),
        Check(
            "Reconciliation",
            "Exact average x count = total, per department",
            True,
            all(abs(sum(s) / len(s) * len(s) - totals[d]) < 1e-6 for d, s in by_department(rows).items()),
        ),
    ]
    return checks, profile(path, rows)


def profile(path: Path, rows: list[dict]) -> dict:
    raw = path.read_bytes()
    names = Counter(r["Emp_Name"] for r in rows)
    it_salaries = by_department(rows)["IT"]
    return {
        "line_endings": "CRLF (Windows)" if b"\r\n" in raw else "LF (Unix)",
        "ends_with_newline": raw.endswith(b"\n"),
        "newline_characters (what wc -l reports)": raw.count(b"\n"),
        "age_range": (min(r["Age"] for r in rows), max(r["Age"] for r in rows)),
        "salary_range": (min(r["Salary"] for r in rows), max(r["Salary"] for r in rows)),
        "duplicate_names (Emp_Name is NOT a key)": {n: c for n, c in names.items() if c > 1},
        "IT median salary (vs 7400 average)": statistics.median(it_salaries),
        "names starting with 'O'": [r["Emp_Name"] for r in rows if r["Emp_Name"].startswith("O")],
    }


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def print_console(checks: list[Check], quality: dict) -> None:
    print("=" * 78)
    print(" VALIDATION REPORT (plain Python, no Spark)")
    print("=" * 78)
    section = None
    for c in checks:
        if c.section != section:
            section = c.section
            print(f"\n[{section}]")
        print(f"  {'PASS' if c.passed else 'FAIL'}  {c.name}")
        if not c.passed:
            print(f"        expected={c.expected!r}\n        actual  ={c.actual!r}")
    print("\n[Data profile]")
    for k, v in quality.items():
        print(f"  - {k}: {v}")
    passed = sum(c.passed for c in checks)
    print("\n" + "=" * 78)
    print(f" {passed}/{len(checks)} checks passed")
    print("=" * 78)


def render_markdown(checks: list[Check], quality: dict) -> str:
    passed = sum(c.passed for c in checks)
    out = [
        "## Validation report (plain Python)", "",
        f"**{passed}/{len(checks)} checks passed**", "",
        "| Section | Check | Result |", "|---|---|---|",
    ]
    out += [f"| {c.section} | {c.name} | {'✅' if c.passed else '❌'} |" for c in checks]
    out += ["", "## Data profile", "", "| Item | Value |", "|---|---|"]
    out += [f"| {k} | `{v}` |" for k, v in quality.items()]
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--summary", type=Path, help="Append a Markdown report (e.g. $GITHUB_STEP_SUMMARY)")
    args = parser.parse_args(argv)

    checks, quality = run_checks(args.data)
    print_console(checks, quality)
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as fh:
            fh.write(render_markdown(checks, quality))
    return 0 if all(c.passed for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
