"""
PySpark tests: every task, plus the lessons learned while building it.

Requires pyspark and Java 17+. Skipped automatically when pyspark is not
installed, so the plain-Python tests still run anywhere.

Run with:  pytest -v tests/test_spark_tasks.py
"""

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.errors import AnalysisException  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import IntegerType, StringType, StructField, StructType  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "validation"))

import employee_analysis as ea  # noqa: E402
import validate as v  # noqa: E402

DATA = ROOT / "data" / "employees.csv"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("employee-analytics-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def df(spark):
    employees_df = ea.load_with_schema(spark, DATA)
    ea.create_view(employees_df)
    return employees_df


@pytest.fixture(scope="session")
def rows():
    """Second witness: the same data loaded with plain Python."""
    return v.load(DATA)


EXPECTED_DTYPES = {"Emp_No": "int", "Emp_Name": "string", "Salary": "int", "Age": "int", "Department": "string"}


# --------------------------------------------------------------------------- #
# Tasks 1-4: loading, schema, view
# --------------------------------------------------------------------------- #
def test_task1_inferred_schema(spark):
    inferred = ea.load_inferred(spark, DATA)
    assert dict(inferred.dtypes) == EXPECTED_DTYPES
    assert inferred.count() == 50


def test_task2_explicit_schema(df):
    assert dict(df.dtypes) == EXPECTED_DTYPES
    first = df.filter("Emp_No = 198").first()
    assert (first.Emp_Name, first.Salary, first.Age) == ("Donald", 2600, 29)


def test_schema_maps_by_position_not_by_name(spark):
    """The bug caught in Task 2: Salary and Age swapped load WITHOUT any error."""
    swapped = StructType([
        StructField("Emp_No", IntegerType()),
        StructField("Emp_Name", StringType()),
        StructField("Age", IntegerType()),      # position 3 is really Salary
        StructField("Salary", IntegerType()),   # position 4 is really Age
        StructField("Department", StringType()),
    ])
    wrong = spark.read.csv(str(DATA), header=True, schema=swapped)
    donald = wrong.filter("Emp_No = 198").first()
    assert donald.Age == 2600 and donald.Salary == 29   # silently corrupted


def test_nullable_false_is_not_enforced_for_csv(spark):
    strict = StructType([StructField(f.name, f.dataType, nullable=False) for f in ea.EMPLOYEE_SCHEMA])
    loaded = spark.read.csv(str(DATA), header=True, schema=strict)
    assert all(field.nullable for field in loaded.schema.fields)


def test_task4_view_is_queryable(spark, df):
    assert spark.sql("SELECT COUNT(*) AS n FROM employees").first().n == 50


# --------------------------------------------------------------------------- #
# Tasks 5-15
# --------------------------------------------------------------------------- #
def test_task5_older_than_30(spark, df):
    result = ea.older_than_30(spark)
    assert result.count() == 37
    # Cross-check: SQL and DataFrame API give the same answer
    assert df.filter(df.Age > 30).count() == 37
    assert result.agg({"Age": "min"}).first()[0] > 30


def test_task6_salary_stats(spark, df):
    stats = {r.Department: r for r in ea.salary_stats_by_department(spark).collect()}
    for dept, avg in v.EXPECTED["avg_salary"].items():
        assert stats[dept].Avg_Salary == pytest.approx(avg)
        assert stats[dept].Min_Salary <= stats[dept].Avg_Salary <= stats[dept].Max_Salary
    assert sum(r.Employee_Count for r in stats.values()) == 50
    assert ea.salary_stats_by_department(spark).first().Department == "IT"


def test_task7_it_employees(df):
    it = ea.it_employees(df)
    assert it.count() == 10
    assert {r.Department for r in it.collect()} == {"IT"}
    assert df.filter(df.Department == "it").count() == 0   # values are case-sensitive


def test_task8_bonus(df):
    bonus = ea.with_bonus(df)
    assert dict(bonus.dtypes)["SalaryAfterBonus"] == "double"
    values = {r.Emp_Name: r.SalaryAfterBonus for r in bonus.filter("Emp_No IN (198, 205)").collect()}
    assert values["Donald"] == pytest.approx(2860.0)
    assert values["Shelley"] == pytest.approx(13208.8)
    # Immutability: the original DataFrame is untouched
    assert "SalaryAfterBonus" not in df.columns


def test_task9_max_salary_by_age(df):
    result = ea.max_salary_by_age(df).collect()
    assert len(result) == 14
    assert (result[0].Age, result[0].Max_Salary) == (39, 24000)
    assert {r.Age: r.Max_Salary for r in result} == {
        26: 3600, 27: 17000, 28: 12008, 29: 10000, 30: 8000, 31: 8200, 32: 13000,
        33: 12008, 34: 7800, 35: 9000, 36: 7900, 37: 17000, 38: 6000, 39: 24000,
    }


def test_task10_self_join(df):
    joined = ea.self_join(df)
    assert joined.count() == 50
    assert len(joined.columns) == 10
    # Without the alias, "Salary" is ambiguous
    with pytest.raises(AnalysisException):
        joined.select("Salary").collect()
    # With the alias, it works
    assert joined.select("e1.Salary").count() == 50


def test_join_on_column_name_keeps_one_key(df):
    assert len(df.alias("a").join(df.alias("b"), "Emp_No").columns) == 9


def test_emp_no_is_unique(df):
    assert df.select("Emp_No").distinct().count() == df.count()


def test_task11_average_age(df):
    assert ea.average_age(df).first().Avg_Age == pytest.approx(33.56)


def test_show_returns_none(df):
    """The Task 11 bug: saving the result of show() saves None."""
    assert ea.average_age(df).show() is None


def test_task12_total_salary(df):
    totals = {r.Department: r.Total_Salary for r in ea.total_salary_by_department(df).collect()}
    assert totals == v.EXPECTED["total_by_dept"]
    assert sum(totals.values()) == 309116


def test_task13_sort(df):
    result = [(r.Age, r.Salary) for r in ea.sort_by_age_and_salary(df).collect()]
    assert result == sorted(result, key=lambda t: (t[0], -t[1]))
    assert [s for a, s in result if a == 39] == [24000, 9000, 8200, 6000, 4800]


def test_task14_count_is_deterministic(df):
    result = [(r.Department, r.Employee_Count) for r in ea.employees_per_department(df).collect()]
    # Finance and IT tie at 10; the Department tiebreaker fixes their order
    assert result == [("Sales", 13), ("Finance", 10), ("IT", 10), ("Marketing", 9), ("HR", 8)]


def test_task15_names_with_o(spark, df):
    names = sorted(r.Emp_Name for r in ea.names_with_o(df).collect())
    assert names == sorted(["Donald", "Douglas", "John", "Jose Manuel", "Mozhe", "Jason", "John", "Joshua"])
    # Cross-check with SQL LIKE
    assert spark.sql("SELECT * FROM employees WHERE Emp_Name LIKE '%o%'").count() == 8


# --------------------------------------------------------------------------- #
# Two witnesses: Spark vs plain Python
# --------------------------------------------------------------------------- #
def test_spark_agrees_with_plain_python(df, rows):
    spark_totals = {r.Department: r.Total_Salary for r in ea.total_salary_by_department(df).collect()}
    spark_counts = {r.Department: r.Employee_Count for r in ea.employees_per_department(df).collect()}
    spark_max = {r.Age: r.Max_Salary for r in ea.max_salary_by_age(df).collect()}
    assert spark_totals == v.total_salary_by_department(rows)
    assert spark_counts == v.count_by_department(rows)
    assert spark_max == v.max_salary_by_age(rows)
    assert ea.names_with_o(df).count() == len(v.names_with_o(rows))
