# Databricks notebook source
# MAGIC %skip
# MAGIC %pip install databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# WGU DQX Profiler + DQ Checker
# Documentation: https://github.com/databricks-labs/dqx

import logging, yaml, os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, year, current_timestamp, max as spark_max, size
from delta.tables import DeltaTable
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition

# Structured logging setup
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# COMMAND ----------

def load_config():
    """Strict YAML config loader with validation."""
    config_path = "/Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_workflow_config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    required_sections = ["table_names", "profiling", "timestamp_year_bounds"]
    for s in required_sections:
        if s not in cfg:
            raise KeyError(f"Missing required section in config: '{s}'")
    required_tables = ["bronze", "dqx_valid", "dqx_quarantine"]
    for t in required_tables:
        if t not in cfg["table_names"]:
            raise KeyError(f"Missing required table name in config['table_names']: '{t}'")
    log.info("✅ Loaded configuration from: %s", config_path)
    return cfg

# COMMAND ----------

@register_rule("row")
def paid_status_consistency(paid_col: str, status_col: str):
    """Custom DQX business rule enforcing logical consistency between 'paid' and 'student_status'."""
    invalid_condition = (
        (F.col(paid_col) == True) & (~F.col(status_col).isin("graduated", "dropped"))
    )
    return make_condition(
        invalid_condition,
        "Paid is TRUE but student_status is not graduated or dropped",
        "paid_status_consistency"
    )

# COMMAND ----------

def upsert_to_table(df, table_name, key="student_id"):
    """Performs an incremental Delta merge (upsert) into the specified target table."""
    count = df.count()
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        log.info("Created new table %s with %d records.", table_name, count)
        return
    delta = DeltaTable.forName(spark, table_name)
    join_key = key if key in df.columns else df.columns[0]
    (
        delta.alias("tgt")
        .merge(df.alias("src"), f"tgt.{join_key} = src.{join_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("Upserted %d → %s", count, table_name)

# COMMAND ----------

def main():
    """DQX profiling + validation workflow."""
    cfg = load_config()

    source_table     = cfg["table_names"]["bronze"]
    valid_table      = cfg["table_names"]["dqx_valid"]
    quarantine_table = cfg["table_names"]["dqx_quarantine"]
    lo, hi           = cfg["timestamp_year_bounds"]

    log.info("Source: %s | Valid: %s | Quarantine: %s", source_table, valid_table, quarantine_table)

    # --- Incremental Load from Bronze ---
    students_df = spark.table(source_table)
    if spark.catalog.tableExists(valid_table):
        last_ts = spark.table(valid_table).agg(spark_max("load_timestamp")).collect()[0][0]
        if last_ts:
            students_df = students_df.filter(col("load_timestamp") > last_ts)
            log.info("Incremental mode — filtering records newer than %s", last_ts)
    else:
        log.info("Full run — no prior valid data detected.")

    if students_df.count() == 0:
        log.warning("No new rows detected. Exiting.")
        dbutils.notebook.exit("No new rows detected.")

    # --- Timestamp Sanity Checks ---
    students_df_filtered = students_df.filter(
        (year(col("load_timestamp")).between(lo, hi)) &
        (year(col("month_end_date")).between(lo, hi))
    )
    log.info("Rows ready for profiling: %d", students_df_filtered.count())

    # --- Profiling + Auto Rules ---
    profiler = DQProfiler(ws)
    summary_stats, profiles = profiler.profile(
        students_df_filtered,
        options=cfg["profiling"]
    )
    auto_rules = DQGenerator(ws).generate_dq_rules(profiles)
    log.info("Profiling complete — %d auto rules generated.", len(auto_rules))

    # --- Manual + Custom Rules ---
    manual_rules = yaml.safe_load(r"""
- check:
    function: is_not_null_and_not_empty
    for_each_column: [student_id, load_timestamp, paid]
  criticality: error
  name: not_null_core_fields

- check:
    function: is_unique
    arguments: { columns: [student_id] }
  criticality: error
  name: unique_student_id

- check:
    function: regex_match
    for_each_column: [student_id]
    arguments: { regex: "^(?i)(?!INVALID|_|NULL)[A-Z]{3}[0-9]{8}$", negate: false }
  criticality: error
  name: valid_student_id_pattern_strict

- check:
    function: regex_match
    for_each_column: [email]
    arguments: { regex: "^(?i)(?!.*\\.\\.)(?!.*\\.@)(?!.*@\\.)[A-Z0-9](?:[A-Z0-9._%+-]{0,63}[A-Z0-9])?@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\\.[A-Z]{2,})+$", negate: false }
  criticality: warn
  name: valid_email_format_strict

- check:
    function: regex_match
    for_each_column: [name]
    arguments: { regex: "^(?!.*\\s{2,})(?!\\s)(?!.*\\s$)[A-Za-z][A-Za-z\\s'\\-]*$", negate: false }
  criticality: warn
  name: valid_name_format_strict

- check:
    function: is_in_list
    for_each_column: [student_status]
    arguments: { allowed: [graduated, current, break, dropped] }
  criticality: error
  name: valid_student_status

- check:
    function: paid_status_consistency
    arguments: { paid_col: paid, status_col: student_status }
  criticality: warn
  name: paid_status_inconsistent_with_status

- check:
    function: regex_match
    for_each_column: [name]
    arguments: { regex: "^[A-Za-z][A-Za-z\\s'\\-]*$", negate: false }
  criticality: error
  name: valid_name_format
""")
    log.info("Loaded %d manual rule definitions.", len(manual_rules))

    # --- Validate + Apply ---
    dq_engine = DQEngine(ws)
    all_rules = manual_rules + auto_rules
    status = dq_engine.validate_checks(all_rules, custom_check_functions=globals())
    if status.has_errors:
        log.error("Rule validation issues detected: %s", status.errors)
        raise ValueError("Rule validation failed.")
    log.info("All rules validated successfully.")

    valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(
        students_df_filtered,
        all_rules,
        custom_check_functions=globals()
    )
    if "student_id" in invalid_df.columns:
        invalid_df = invalid_df.dropDuplicates(["student_id"])

    # --- Write to Delta ---
    valid_df  = valid_df.withColumn("dq_run_timestamp", current_timestamp())
    invalid_df = invalid_df.withColumn("dq_run_timestamp", current_timestamp())
    upsert_to_table(valid_df, valid_table)
    upsert_to_table(invalid_df, quarantine_table)

    # --- Summary ---
    total_rows = students_df_filtered.count()
    total_warnings = invalid_df.filter(size(col("_warnings")) > 0).count()
    total_errors = invalid_df.filter(size(col("_errors")) > 0).count()
    clean_df = valid_df.filter(
        ((col("_warnings").isNull()) | (size(col("_warnings")) == 0)) &
        ((col("_errors").isNull())   | (size(col("_errors")) == 0))
    )
    clean_count = clean_df.count()

    log.info("=" * 70)
    log.info("FINAL DQX SUMMARY")
    log.info("Run Completed: %s", datetime.now())
    log.info("Rows Profiled: %d | Warnings: %d | Errors: %d | Clean: %d",
             total_rows, total_warnings, total_errors, clean_count)
    log.info("=" * 70)

    summary_df = spark.createDataFrame(
        [
            ("Total Rows Profiled", total_rows),
            ("Total Warnings Found", total_warnings),
            ("Total Errors Found", total_errors),
            ("Fully Clean Records", clean_count),
        ],
        ["Metric", "Count"]
    )
    display(summary_df)

# COMMAND ----------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Workflow failed: %s", e)
        raise
