"""
Employee data analysis with PySpark DataFrames and Spark SQL.

One function per project task, so the same logic is used by the notebook,
the command line and the unit tests.

Run:
    python src/employee_analysis.py                 # uses data/employees.csv
    python src/employee_analysis.py path/to/file.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
# "functions as F" instead of "from ... import max, sum, round":
# importing those names directly would shadow Python's built-in max/sum/round.
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

DEFAULT_DATA = Path(__file__).resolve().parent.parent / "data" / "employees.csv"

# Task 2 - explicit schema.
# Spark maps a user schema to CSV columns BY POSITION (header=True only skips
# the first line), so the order must match the file:
#   Emp_No, Emp_Name, Salary, Age, Department
# Salary and Age are both integers, so swapping them would load without any
# error and silently corrupt every salary/age result.
EMPLOYEE_SCHEMA = StructType([
    StructField("Emp_No", IntegerType()),
    StructField("Emp_Name", StringType()),
    StructField("Salary", IntegerType()),
    StructField("Age", IntegerType()),
    StructField("Department", StringType()),
])


def get_spark(app_name: str = "Employee Analytics") -> SparkSession:
    """Local Spark session (one partition for shuffles: the dataset is tiny)."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def load_inferred(spark: SparkSession, path: str | Path = DEFAULT_DATA) -> DataFrame:
    """Task 1 - let Spark guess the types (costs an extra pass over the data)."""
    return spark.read.csv(str(path), header=True, inferSchema=True)


def load_with_schema(spark: SparkSession, path: str | Path = DEFAULT_DATA) -> DataFrame:
    """Task 2 - read with the explicit schema: no guessing, no extra pass."""
    return spark.read.csv(str(path), header=True, schema=EMPLOYEE_SCHEMA)


def create_view(df: DataFrame, name: str = "employees") -> None:
    """Task 4 - name tag for Spark SQL. createOrReplace = idempotent re-runs.

    The view points to THIS DataFrame. Re-run after reassigning the variable.
    """
    df.createOrReplaceTempView(name)


# --------------------------------------------------------------------------- #
# Tasks 5-15
# --------------------------------------------------------------------------- #
def older_than_30(spark: SparkSession) -> DataFrame:
    """Task 5 - SQL on the view. 'exceeds 30' -> strictly greater than."""
    return spark.sql("""
        SELECT *
        FROM employees
        WHERE Age > 30
    """)


def salary_stats_by_department(spark: SparkSession) -> DataFrame:
    """Task 6 - average salary per department, with count/min/max as a sanity check.

    Every average must fall between its min and max, and the counts must add
    up to the total number of rows.
    """
    return spark.sql("""
        SELECT Department,
               ROUND(AVG(Salary), 2) AS Avg_Salary,
               COUNT(*)              AS Employee_Count,
               MIN(Salary)           AS Min_Salary,
               MAX(Salary)           AS Max_Salary
        FROM employees
        GROUP BY Department
        ORDER BY Avg_Salary DESC, Department ASC
    """)


def it_employees(df: DataFrame) -> DataFrame:
    """Task 7 - DataFrame API filter. Values are case-sensitive: 'IT' != 'it'."""
    return df.filter(F.col("Department") == "IT")


def with_bonus(df: DataFrame) -> DataFrame:
    """Task 8 - new column with a 10% bonus.

    1.10 has no exact binary representation, so 2600 * 1.10 = 2860.0000000000005.
    Rounding fixes the display; real money columns should use DecimalType.
    withColumn returns a NEW DataFrame (DataFrames are immutable).
    """
    return df.withColumn("SalaryAfterBonus", F.round(F.col("Salary") * 1.10, 2))


def max_salary_by_age(df: DataFrame) -> DataFrame:
    """Task 9 - one group per age. No orderBy = no guaranteed order."""
    return (
        df.groupBy("Age")
        .agg(F.max("Salary").alias("Max_Salary"))
        .orderBy(F.col("Age").desc())
    )


def self_join(df: DataFrame) -> DataFrame:
    """Task 10 - join the DataFrame with itself on Emp_No.

    Aliases tell the two sides apart; without them every column name is
    ambiguous. The result keeps BOTH sides: 10 columns.
    (df.join(df2, "Emp_No") would keep a single Emp_No column: 9 columns.)
    """
    e1 = df.alias("e1")
    e2 = df.alias("e2")
    return e1.join(e2, F.col("e1.Emp_No") == F.col("e2.Emp_No"))


def average_age(df: DataFrame) -> DataFrame:
    """Task 11 - one number for the whole table: agg() without groupBy."""
    return df.agg(F.round(F.avg("Age"), 2).alias("Avg_Age"))


def total_salary_by_department(df: DataFrame) -> DataFrame:
    """Task 12 - total salary per department."""
    return (
        df.groupBy("Department")
        .agg(F.sum("Salary").alias("Total_Salary"))
        .orderBy(F.col("Total_Salary").desc())
    )


def sort_by_age_and_salary(df: DataFrame) -> DataFrame:
    """Task 13 - ONE sort with two keys: Age ascending, Salary descending.

    Two chained sorts would not combine; directions belong to columns.
    """
    return df.orderBy(F.col("Age").asc(), F.col("Salary").desc())


def employees_per_department(df: DataFrame) -> DataFrame:
    """Task 14 - count(*) counts rows (people); count(col) skips nulls.

    Department is a tiebreaker so equal counts always come out in the same order.
    """
    return (
        df.groupBy("Department")
        .agg(F.count("*").alias("Employee_Count"))
        .orderBy(F.col("Employee_Count").desc(), F.col("Department").asc())
    )


def names_with_o(df: DataFrame) -> DataFrame:
    """Task 15 - names containing 'o', case-insensitive (robust to 'Oscar')."""
    return df.filter(F.lower(F.col("Emp_Name")).contains("o"))


# --------------------------------------------------------------------------- #
# Run everything
# --------------------------------------------------------------------------- #
def main(path: str | Path = DEFAULT_DATA) -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\nTask 1 - inferred schema")
    load_inferred(spark, path).printSchema()

    employees_df = load_with_schema(spark, path)
    print(f"Task 2/3 - explicit schema ({employees_df.count()} rows)")
    employees_df.printSchema()

    create_view(employees_df)

    steps = [
        ("Task 5 - Age > 30", older_than_30(spark)),
        ("Task 6 - Salary stats by department", salary_stats_by_department(spark)),
        ("Task 7 - IT employees", it_employees(employees_df)),
        ("Task 8 - Salary after 10% bonus", with_bonus(employees_df)),
        ("Task 9 - Max salary by age", max_salary_by_age(employees_df)),
        ("Task 10 - Self-join on Emp_No", self_join(employees_df)),
        ("Task 11 - Average age", average_age(employees_df)),
        ("Task 12 - Total salary by department", total_salary_by_department(employees_df)),
        ("Task 13 - Sorted by Age asc, Salary desc", sort_by_age_and_salary(employees_df)),
        ("Task 14 - Employees per department", employees_per_department(employees_df)),
        ("Task 15 - Names containing 'o'", names_with_o(employees_df)),
    ]
    for title, result in steps:
        print(f"\n{title} ({result.count()} rows)")
        result.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DATA)
