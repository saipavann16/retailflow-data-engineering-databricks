# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Read, join, and report
# MAGIC
# MAGIC **Learner scaffold · RetailFlow dataset 1.0.0**
# MAGIC
# MAGIC Course lessons: Day 1, Lessons 3, 5, and 6.
# MAGIC
# MAGIC Goal: Inspect raw CSV fields, preserve guest sales, and produce a first sales report.
# MAGIC
# MAGIC Before running: Run 00 successfully. Teach notebook 03 between the reading and joining sections as Lesson 4.
# MAGIC
# MAGIC Writes: No persistent tables; this notebook explores the canonical clean batch.
# MAGIC
# MAGIC Follow the playlist module order. Build Buddy will help you create the missing numbered exercise cells. The instructor has a separate completed reference. Step IDs are stable anchors for that authoring work. Saved outputs are blank unless explicitly verified locally; a passed local check does not imply Databricks execution.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your notebook grows through Build Buddy
# MAGIC
# MAGIC Configuration and instructor utility cells are supplied so you can focus on the learning task. The numbered exercise cells have no solutions. Read the playlist instructions, attempt each cell, run it, explain its output, then ask for a hint if needed. A blank exercise cell is incomplete work. Do not use Run All expecting this scaffold to be a finished pipeline. The course revisits notebooks: Lesson 5 runs 02-02, 02-03 and 02-05; Lesson 6 then runs 02-04.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Use the same assigned names in every notebook. Each notebook defines its own variables and helpers; it does not depend on another notebook’s Python memory. Persistent tables are the handoff between notebooks.
# MAGIC

# COMMAND ----------

# Edit these four values in EVERY notebook before running it.
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "retailflow_files"
TABLE_PREFIX = "rf_retailflow"

import re
for name in [CATALOG, SCHEMA, VOLUME, TABLE_PREFIX]:
    assert re.fullmatch(r"[a-z][a-z0-9_]*", name), "Use lowercase letters, digits, and underscores; start with a letter."
assert not any(name.startswith("your_") for name in [CATALOG, SCHEMA, VOLUME]), "Replace the example names with your assigned names."
assert TABLE_PREFIX != "rf_yourname", "Choose the table prefix assigned to you."
DATASET_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/retailflow-v1.0.0"
print("Dataset:", DATASET_ROOT)
print("Table prefix:", f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}_")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplied support cells
# MAGIC
# MAGIC These utilities keep repeated setup and checks in one place inside this notebook. The Python cleaner deliberately processes this tiny fixture on the driver so beginners can inspect each row. Spark handles DataFrames, joins, summaries, and Delta tables. This is a teaching pattern for 80 sales, not a large-scale ingestion design. The helper definitions below are part of the notebook and can be inspected. Learners will construct selected parts through Build Buddy in the later authoring phase.
# MAGIC

# COMMAND ----------

from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from datetime import date, datetime
from collections import Counter, defaultdict
import csv, json, re

def read_rows(relative_path):
    with (Path(DATASET_ROOT) / relative_path).open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))

def money(value):
    return Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def business_payload(row):
    return {key: value for key, value in row.items() if key != 'source_row_id'}

def business_records(rows):
    return sorted([business_payload(row) for row in rows], key=lambda row: row['sale_id'])

def dimension_maps():
    return (
        {r['store_id']: r for r in read_rows('data/stores.csv')},
        {r['product_id']: r for r in read_rows('data/products.csv')},
        {r['customer_id']: r for r in read_rows('data/customers.csv')},
    )

def clean_sales(raw_rows):
    """Readable Python cleaning for this tiny fixture; no distributed UDF required."""
    stores, products, customers = dimension_maps()
    candidates, rejected, repaired = [], [], []
    for raw in sorted(raw_rows, key=lambda row: row['source_row_id']):
        row = {key: value.strip() for key, value in raw.items()}
        for key in ['store_id', 'product_id', 'customer_id', 'payment_method']:
            row[key] = row[key].upper()
        if row['customer_id'] == 'UNKNOWN':
            row['customer_id'] = ''
        reason = None
        if any(not row[k] for k in ['sale_id', 'source_row_id', 'sale_date', 'store_id', 'product_id']):
            reason = 'REQUIRED_MISSING'
        if not reason:
            try:
                fmt = '%d/%m/%Y' if '/' in row['sale_date'] else '%Y-%m-%d'
                pattern = r'\d{2}/\d{2}/\d{4}' if '/' in row['sale_date'] else r'\d{4}-\d{2}-\d{2}'
                if not re.fullmatch(pattern, row['sale_date']):
                    raise ValueError('Date format mismatch')
                row['sale_date'] = datetime.strptime(row['sale_date'], fmt).date().isoformat()
            except ValueError:
                reason = 'DATE_INVALID'
        if not reason:
            if row['product_id'] not in products:
                reason = 'PRODUCT_UNKNOWN'
            elif row['store_id'] not in stores:
                reason = 'STORE_UNKNOWN'
            elif row['customer_id'] and row['customer_id'] not in customers:
                reason = 'CUSTOMER_UNKNOWN'
        if not reason:
            try:
                if not re.fullmatch(r'\d+', row['quantity']) or int(row['quantity']) < 1:
                    reason = 'QUANTITY_INVALID'
                elif not re.fullmatch(r'\d+', row['record_version']) or int(row['record_version']) < 1:
                    reason = 'VERSION_INVALID'
                else:
                    row['unit_price'] = format(money(row['unit_price'] or products[row['product_id']]['list_price']), '.2f')
                    row['discount_pct'] = row['discount_pct'] or '0'
                    price, discount = Decimal(row['unit_price']), Decimal(row['discount_pct'])
                    if not price.is_finite() or price <= 0:
                        reason = 'PRICE_INVALID'
                    elif not discount.is_finite() or not 0 <= discount <= 100:
                        reason = 'DISCOUNT_INVALID'
                    elif row['payment_method'] not in ['UPI', 'CARD', 'CASH']:
                        reason = 'PAYMENT_INVALID'
            except (ValueError, InvalidOperation):
                reason = 'NUMBER_INVALID'
        if reason:
            rejected.append(dict(raw, rejection_reason=reason))
        else:
            if row != raw:
                repaired.append(row['source_row_id'])
            candidates.append(row)
    groups = defaultdict(list)
    for row in candidates:
        groups[(row['sale_id'], row['record_version'])].append(row)
    accepted, duplicate_ids = [], []
    raw_by_id = {row['source_row_id']: row for row in raw_rows}
    for group in groups.values():
        if any(business_payload(row) != business_payload(group[0]) for row in group):
            rejected.extend(dict(raw_by_id[row['source_row_id']], rejection_reason='VERSION_CONFLICT') for row in group)
        else:
            accepted.append(group[0])
            duplicate_ids.extend(row['source_row_id'] for row in group[1:])
    return accepted, rejected, duplicate_ids, repaired

def current_versions(rows):
    state = {}
    for row in rows:
        old = state.get(row['sale_id'])
        if old is None or int(row['record_version']) > int(old['record_version']):
            state[row['sale_id']] = row
    return sorted(state.values(), key=lambda row: row['sale_id'])

def visible_returns(stage, sales_rows):
    rows = [r for r in read_rows('data/returns.csv') if stage == 'final' or r['available_batch'] == '01']
    sales = {row['sale_id']: row for row in sales_rows}
    assert len({r['return_id'] for r in rows}) == len(rows)
    returned = Counter()
    for row in rows:
        sale = sales[row['sale_id']]
        assert row['available_batch'] in ['01', '02']
        assert date.fromisoformat(row['return_date']) >= date.fromisoformat(sale['sale_date'])
        quantity = int(row['return_quantity'])
        assert quantity > 0
        returned[row['sale_id']] += quantity
        assert returned[row['sale_id']] <= int(sale['quantity'])
        assert Decimal(row['refund_amount']) == money(quantity * Decimal(sale['unit_price']) * (1 - Decimal(sale['discount_pct']) / 100))
    return rows


# COMMAND ----------

from pyspark.sql import functions as F, types as T

SALES_COLUMNS = ['source_row_id', 'sale_id', 'sale_date', 'store_id', 'product_id', 'customer_id', 'quantity', 'unit_price', 'discount_pct', 'payment_method', 'record_version']
SALES_SCHEMA = T.StructType([
    T.StructField(name, T.DateType() if name == 'sale_date' else
                  T.IntegerType() if name in ['quantity', 'record_version'] else
                  T.DecimalType(12, 2) if name == 'unit_price' else
                  T.DecimalType(5, 2) if name == 'discount_pct' else T.StringType(), True)
    for name in SALES_COLUMNS
])

def sales_frame(rows):
    typed = []
    for row in rows:
        values = []
        for name in SALES_COLUMNS:
            value = row[name]
            if name == 'sale_date': value = date.fromisoformat(value)
            elif name in ['quantity', 'record_version']: value = int(value)
            elif name in ['unit_price', 'discount_pct']: value = Decimal(value)
            elif name == 'customer_id' and value == '': value = None
            values.append(value)
        typed.append(tuple(values))
    return spark.createDataFrame(typed, SALES_SCHEMA)

def raw_frame(relative_path):
    return spark.read.option('header', True).option('inferSchema', False).option('mode', 'FAILFAST').csv(f'{DATASET_ROOT}/{relative_path}')

def table_name(suffix):
    assert re.fullmatch(r'[a-z][a-z0-9_]*', suffix)
    return f'{CATALOG}.{SCHEMA}.{TABLE_PREFIX}_{suffix}'

def assert_expected(frame, relative_path, ignore_provenance=False):
    columns = [c for c in frame.columns if not (ignore_provenance and c == 'source_row_id')]
    actual = frame.select(*columns)
    # Read full files before dropping provenance so schema fields match CSV positions.
    expected = spark.read.schema(frame.schema).option('header', True).option('mode', 'FAILFAST').csv(f'{DATASET_ROOT}/checks/expected/{relative_path}').select(*columns)
    assert actual.exceptAll(expected).limit(1).count() == 0, f'Extra/different rows: {relative_path}'
    assert expected.exceptAll(actual).limit(1).count() == 0, f'Missing rows: {relative_path}'
    print('PASS:', relative_path)

def collected_sales(frame):
    rows = []
    for record in frame.select(*SALES_COLUMNS).collect():
        row = record.asDict()
        row['sale_date'] = row['sale_date'].isoformat()
        row['quantity'] = str(row['quantity'])
        row['record_version'] = str(row['record_version'])
        row['unit_price'] = format(row['unit_price'], '.2f')
        row['discount_pct'] = format(row['discount_pct'].normalize(), 'f')
        row['customer_id'] = row['customer_id'] or ''
        rows.append(row)
    return sorted(rows, key=lambda row: row['sale_id'])

def merge_counts(current, incoming):
    joined = incoming.alias('s').join(current.alias('t'), F.col('s.sale_id') == F.col('t.sale_id'), 'left')
    business_columns = [c for c in SALES_COLUMNS if c != 'source_row_id']
    equal = F.lit(True)
    for name in business_columns:
        equal = equal & F.col(f's.{name}').eqNullSafe(F.col(f't.{name}'))
    conflicts = joined.where((F.col('s.record_version') == F.col('t.record_version')) & ~equal)
    assert conflicts.limit(1).count() == 0, 'Equal-version business conflict: investigate before merge.'
    counts = {'insert': 0, 'update': 0, 'noop': 0}
    for row in joined.select(
        F.when(F.col('t.sale_id').isNull(), 'insert')
         .when(F.col('s.record_version') > F.col('t.record_version'), 'update')
         .otherwise('noop').alias('action')
    ).groupBy('action').count().collect():
        counts[row['action']] = row['count']
    return counts

def merge_into_silver(incoming):
    from delta.tables import DeltaTable
    assert incoming.groupBy('sale_id').count().where('count > 1').limit(1).count() == 0
    target = table_name('silver_sales')
    counts = merge_counts(spark.table(target), incoming)
    (DeltaTable.forName(spark, target).alias('t')
        .merge(incoming.alias('s'), 't.sale_id = s.sale_id')
        .whenMatchedUpdateAll(condition='s.record_version > t.record_version')
        .whenNotMatchedInsertAll().execute())
    return counts


# COMMAND ----------

# MAGIC %md
# MAGIC ## 02-01: Read rows as strings
# MAGIC
# MAGIC **Core concept Vibe:** A schema describes column names and types.
# MAGIC
# MAGIC **Build Buddy action:** Read initial sales without automatic type guessing and inspect the first records.
# MAGIC
# MAGIC **Expected checkpoint:** 60 rows; all raw fields are strings. Stop here to teach notebook 03 for Lesson 4.
# MAGIC

# COMMAND ----------

raw_sales = raw_frame('data/sales_batch_01.csv')
raw_sales.printSchema()
display(raw_sales.orderBy('source_row_id').limit(6))
assert raw_sales.count() == 60

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02-02: Cast checked values
# MAGIC
# MAGIC **Core concept Vibe:** Validate before turning text into dates or numbers.
# MAGIC
# MAGIC **Build Buddy action:** Validate initial sales and create a typed Spark DataFrame.
# MAGIC
# MAGIC **Expected checkpoint:** 60 typed sales; 10 guest sales remain valid.
# MAGIC

# COMMAND ----------

# Build Buddy step 02-02
# Create your solution here after attempting the task.
# Keep this cell ID; it links your work to the playlist.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 02-03: Join by identifiers
# MAGIC
# MAGIC **Core concept Vibe:** A left join preserves every sale, including guests.
# MAGIC
# MAGIC **Build Buddy action:** Enrich sales with store, product and customer names.
# MAGIC
# MAGIC **Expected checkpoint:** 60 enriched sales; S0001 resolves to Asha; S0006 remains present without a customer name.
# MAGIC

# COMMAND ----------

# Build Buddy step 02-03
# Create your solution here after attempting the task.
# Keep this cell ID; it links your work to the playlist.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 02-04: Calculate and group sales
# MAGIC
# MAGIC **Core concept Vibe:** Grouping combines records that share a key.
# MAGIC
# MAGIC **Build Buddy action:** Calculate discounted sales and summarize them by store.
# MAGIC
# MAGIC **Expected checkpoint:** Total net sales before returns = INR 14,456.00; every store agrees with the answer key.
# MAGIC

# COMMAND ----------

# Build Buddy step 02-04
# Create your solution here after attempting the task.
# Keep this cell ID; it links your work to the playlist.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 02-05: Show products with no sales
# MAGIC
# MAGIC **Core concept Vibe:** Starting from the product list preserves unsold products.
# MAGIC
# MAGIC **Build Buddy action:** Left join product totals onto all 12 products.
# MAGIC
# MAGIC **Expected checkpoint:** P012 Desk Lamp appears with zero units sold.
# MAGIC

# COMMAND ----------

# Build Buddy step 02-05
# Create your solution here after attempting the task.
# Keep this cell ID; it links your work to the playlist.
