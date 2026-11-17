"""
Unit tests for the plain-Python validation layer (no Spark needed).

Run with:  pytest -v tests/test_validate.py
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))

import validate as v  # noqa: E402

DATA = ROOT / "data" / "employees.csv"


@pytest.fixture
def tiny():
    """Four employees, checkable by eye."""
    return [
        {"Emp_No": 1, "Emp_Name": "Oscar", "Salary": 1000, "Age": 30, "Department": "IT"},
        {"Emp_No": 2, "Emp_Name": "Ana", "Salary": 3000, "Age": 31, "Department": "IT"},
        {"Emp_No": 3, "Emp_Name": "John", "Salary": 2000, "Age": 31, "Department": "HR"},
        {"Emp_No": 4, "Emp_Name": "Lia", "Salary": 500, "Age": 45, "Department": "HR"},
    ]


def test_group_aggregates(tiny):
    assert v.avg_salary_by_department(tiny) == {"IT": 2000.0, "HR": 1250.0}
    assert v.total_salary_by_department(tiny) == {"IT": 4000, "HR": 2500}
    assert v.count_by_department(tiny) == {"IT": 2, "HR": 2}


def test_max_salary_by_age_keeps_the_highest(tiny):
    assert v.max_salary_by_age(tiny) == {30: 1000, 31: 3000, 45: 500}


def test_names_with_o_is_case_insensitive(tiny):
    # "Oscar" starts with a capital O: a case-sensitive filter would miss him
    assert v.names_with_o(tiny) == ["Oscar", "John"]


def test_self_join_multiplies_duplicate_keys():
    # A key that appears twice matches 2 x 2 = 4 times
    rows = [{"Emp_No": 1}, {"Emp_No": 1}, {"Emp_No": 2}]
    assert v.self_join_count(rows) == 5


def test_float_bonus_needs_rounding():
    # 1.10 has no exact binary representation (IEEE 754)
    assert 2600 * 1.10 != 2860.0
    assert round(2600 * 1.10, 2) == 2860.0


def test_all_checks_pass_on_project_data():
    checks, _ = v.run_checks(DATA)
    failed = [c.name for c in checks if not c.passed]
    assert failed == []


def test_profile_explains_wc_l():
    _, quality = v.run_checks(DATA)
    # 50 newline characters, no newline after the last row -> wc -l says 50
    # while the file has 51 lines (1 header + 50 employees)
    assert quality["newline_characters (what wc -l reports)"] == 50
    assert quality["ends_with_newline"] is False
    assert quality["duplicate_names (Emp_Name is NOT a key)"] == {
        "Michael": 2, "Steven": 2, "Alexander": 2, "John": 2, "James": 2,
    }


def test_main_exit_code():
    assert v.main(["--data", str(DATA)]) == 0
