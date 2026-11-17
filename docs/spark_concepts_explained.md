# Spark Concepts, Explained

The ideas behind every task in this project, each with an analogy, a small diagram, and where it showed up.

---

## 1. Lazy evaluation: the kitchen ticket

**Analogy:** waiters write orders on a ticket, but the chef cooks nothing until someone says **"serve it!"**. Then the chef reads the whole ticket, plans the most efficient way to cook it, and does the work.

```
TRANSFORMATIONS (written on the ticket, nothing runs)
  filter · withColumn · groupBy · agg · join · orderBy · select
                     │
                     ▼  builds a plan, instantly
ACTIONS (serve it! the work actually happens)
  show · count · collect · first · write
```

**Consequences**
- A `filter` on a huge file returns instantly: nothing has been read yet.
- Some errors only appear when an action runs.
- **Every action cooks from scratch.** A cell with `show()` then `count()` reads the CSV twice:

```
older = spark.sql(...)   → ticket written
older.show()             → read CSV → filter → show
older.count()            → read CSV → filter → count   (all over again)
```

On large data, `.cache()` keeps the result after the first action.

*Seen in:* Task 5 (two actions in one cell).

---

## 2. Immutability: printed photos

**Analogy:** a DataFrame is a printed photo. You can't edit it; you can only take a **new** photo and keep it in the same album slot (variable) or a new one.

```
Task 1:  employees_df ──► [DataFrame A: inferred types]
Task 2:  employees_df ──► [DataFrame B: explicit schema]   A is forgotten, never modified
Task 8:  bonus_df     ──► [DataFrame C: B + SalaryAfterBonus]
         employees_df ──► still DataFrame B (5 columns)
```

**Consequences**
- `withColumn` doesn't add a column *to* a DataFrame; it returns a new one. If you don't save it, it's gone.
- Saving the result of an **action** saves its return value: `df.agg(...).show()` returns `None`.

*Seen in:* Tasks 2, 8, 11.

---

## 3. Schemas: inferred vs explicit

```
inferSchema=True   → Spark reads the data an extra time to GUESS types
schema=...         → you DECIDE the types; no guessing, no extra pass
```

**Explicit schemas map by position, not by name.** `header=True` only skips the first line:

```
CSV header:   Emp_No | Emp_Name | Salary | Age  | Department
Schema:       Emp_No | Emp_Name | Age    | Salary | Department   ← swapped
Result:       Donald's "Age" = 2600, "Salary" = 29 — and NO error
```

Both columns are integers, so the wrong schema loads cleanly and every salary/age result is silently wrong.

**`nullable=False` is not enforced for CSV sources:** Spark reads every column as nullable. It documents intent, but it's not a data quality check.

*Seen in:* Tasks 1–3 (the swap happened in the first attempt; a test now guards it).

---

## 4. Temporary views: name tags

**Analogy:** SQL can only see things with a **name tag** in Spark's catalog. `employees_df` is a Python variable; SQL has no idea it exists.

```
Python world                    SQL world (Spark catalog)
employees_df ──► [DataFrame B] ◄── "employees"  (name tag)
```

- A view **copies no data**; it points to the DataFrame's plan.
- It points to the **photo, not the album slot**: reassign `employees_df` and the view still points to the old DataFrame.
- `createOrReplaceTempView` is **idempotent** (safe to re-run); `createTempView` fails if the name exists.

*Seen in:* Task 4.

---

## 5. One engine, two languages

```
spark.sql("SELECT * FROM employees WHERE Age > 30")  ─┐
                                                       ├─► Catalyst optimizer ─► same plan
employees_df.filter(F.col("Age") > 30)               ─┘
```

Neither is faster. Use whichever is clearer, and use the other one as a **cross-check**.

Three ways to refer to a column in the DataFrame API:

```
df.filter("Age > 30")            ← SQL expression string
df.filter(df.Age > 30)           ← column attribute
df.filter(F.col("Age") > 30)     ← col() function (preferred: no dependence on a variable name)
```

*Seen in:* Tasks 5, 11, 15.

---

## 6. Ordering: no `ORDER BY`, no promise

Without `orderBy`, Spark returns groups in whatever order they come out of the **shuffle** (the step that moves rows with the same key to the same place). Like Cassandra's token order, it looks random and can change between runs.

**Ties need a tiebreaker:** Finance and IT both have 10 employees. Ordering by count alone lets them swap; adding `Department` as a second key makes the output deterministic.

**Directions are sticky notes on columns, not on DataFrames:**

```
employees_df.sort("Age").asc()         ❌ DataFrames don't have .asc()
employees_df.col("Age")                ❌ means "the column named col"
F.col("Age").asc()                     ✅ a column with its direction

employees_df.orderBy( F.col("Age").asc(), F.col("Salary").desc() )
                       main order          tiebreaker inside each age
```

And a Python detail: `a, b` creates a **tuple**. `orderBy(tuple)` fails; pass the columns as separate arguments (or unpack with `*`).

*Seen in:* Tasks 6, 9, 13, 14.

---

## 7. Joins: aliases and duplicate keys

A self-join keeps **both** sides, so every column name appears twice:

```
e1: Emp_No | Emp_Name | Salary | Age | Department
e2:                                              Emp_No | Emp_Name | Salary | Age | Department
→ 10 columns;  select("Salary") → AMBIGUOUS_REFERENCE;  select("e1.Salary") ✅
```

Joining on the column **name** (`e1.join(e2, "Emp_No")`) keeps a single key column (9 columns), like SQL's `JOIN ... USING`.

**Duplicate keys multiply rows:** a key that appears twice matches 2 × 2 = 4 times. Always check uniqueness first:

```
df.select("Emp_No").distinct().count() == df.count()   → unique
```

(In this dataset `Emp_No` is unique, but **`Emp_Name` is not**: five names appear twice. Joining on names would duplicate rows.)

**Real use of a self-join:** employee → manager hierarchies (`e.Manager_No = m.Emp_No`).

*Seen in:* Task 10.

---

## 8. Numbers: floats, rounding, money

`0.1` has no exact binary representation, just like `1/3` has none in decimal:

```
1.10 stored as ≈ 1.100000000000000088817...
2600 × 1.10   = 2860.0000000000005
```

- `F.round(..., 2)` fixes the display.
- **Money belongs in `DecimalType`**, never floats.
- **Round at the end, for display only.** Rounded averages can't rebuild exact totals: Marketing's `6633.33 × 9 = 59699.97`, not `59700`.

*Seen in:* Tasks 6, 8, 12.

---

## 9. Counting and aggregating

| Expression | Counts |
|---|---|
| `count("*")` | rows (people) |
| `count("Emp_No")` | non-null values in that column |
| `groupBy(...).count()` | shortcut; column is named `count` |
| `groupBy(...).agg(F.count("*").alias(...))` | same, with your own name |

**Averages hide outliers:** IT has the highest average (7,400) because one person earns 24,000. Its median is 5,400.

*Seen in:* Tasks 6, 11, 12, 14.

---

## 10. Case sensitivity

| Thing | Case-sensitive? |
|---|---|
| Column names in Spark SQL / DataFrame API | **No** (by default): `age` finds `Age` |
| Values | **Yes**: `'IT'` ≠ `'it'` |
| Quotes in Python | `'IT'` and `"IT"` are identical |
| Quotes in Spark SQL | `'IT'` = value, `` `Department` `` = name (backticks) |

`F.lower(F.col("Emp_Name")).contains("o")` makes the filter case-insensitive, so a future "Oscar" isn't missed.

*Seen in:* Tasks 5, 7, 15.

---

## 11. Shadowed built-ins

```python
from pyspark.sql.functions import max, sum, round   # replaces Python's max, sum, round
from pyspark.sql import functions as F              # safe: F.max, F.sum, F.round
```

After the first import, `round(3.14159, 2)` on a plain number fails in confusing ways. The `F.` prefix makes every call explicit.

*Seen in:* Tasks 8, 9, 12 (the course notebook imports them directly).
