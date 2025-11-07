# Databricks notebook source
# MAGIC %skip
# MAGIC %pip install pyyaml faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# WGU Data Enrichment Workflow
# Docs: https://github.com/databricks-labs/dqx

import logging, os, yaml, random, pandas as pd
from datetime import datetime
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def load_config():
    config_path = "/Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_workflow_config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    required_sections = ["table_names"]
    for s in required_sections:
        if s not in cfg:
            raise KeyError(f"Missing required section: '{s}'")
    required_keys = ["bronze", "bronze_location", "dqx_valid", "silver_enriched", "gold_current"]
    for k in required_keys:
        if k not in cfg["table_names"]:
            raise KeyError(f"Missing required key in table_names: '{k}'")
    log.info("Loaded configuration from: %s", config_path)
    return cfg

# COMMAND ----------

def generate_location_data(new_student_ids):
    fake = Faker("en_US")
    Faker.seed(int(datetime.now().timestamp()))
    rows = [{
        "student_id": sid,
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zipcode": fake.zipcode()[:5],
    } for sid in new_student_ids]
    return pd.DataFrame(rows)

# COMMAND ----------

def main():
    cfg = load_config()

    SOURCE_TABLE         = cfg["table_names"]["bronze"]
    TARGET_TABLE         = cfg["table_names"]["bronze_location"]
    VALID_STUDENTS_TABLE = cfg["table_names"]["dqx_valid"]
    ENRICHED_TABLE       = cfg["table_names"]["silver_enriched"]
    GOLD_TABLE           = cfg["table_names"]["gold_current"]

    log.info("Source: %s | Location: %s | Valid: %s | Enriched: %s | Gold: %s",
             SOURCE_TABLE, TARGET_TABLE, VALID_STUDENTS_TABLE, ENRICHED_TABLE, GOLD_TABLE)

    # --- Bronze → location (incremental) ---
    src_df = spark.table(SOURCE_TABLE).select("student_id").distinct()
    if spark.catalog.tableExists(TARGET_TABLE):
        tgt_df = spark.table(TARGET_TABLE).select("student_id").distinct()
        new_df = src_df.join(tgt_df, on="student_id", how="left_anti")
    else:
        new_df = src_df

    new_count = new_df.count()
    if new_count == 0:
        log.info("No new students to process for location. Exiting.")
        dbutils.notebook.exit("No new rows detected.")

    new_ids = [r["student_id"] for r in new_df.collect()]
    loc_pdf = generate_location_data(new_ids)
    loc_sdf = spark.createDataFrame(loc_pdf)
    mode = "append" if spark.catalog.tableExists(TARGET_TABLE) else "overwrite"
    loc_sdf.write.format("delta").mode(mode).saveAsTable(TARGET_TABLE)
    log.info("Location upsert complete → %s (added %d)", TARGET_TABLE, loc_sdf.count())

    # --- Silver Enriched (join valid + location) ---
    valid_df = spark.table(VALID_STUDENTS_TABLE)
    location_df = spark.table(TARGET_TABLE)
    enriched_df = (
        valid_df.alias("v")
        .join(location_df.alias("l"), on="student_id", how="left")
        .select(
            "v.student_id", "v.name", "v.email", "v.student_status",
            "v.month_end_date", "v.paid", "v.stays_on_campus",
            "v.load_timestamp", "v.dq_run_timestamp",
            "l.city", "l.state", "l.zipcode",
        )
    )
    enriched_count = enriched_df.count()
    enriched_df.write.format("delta").mode("overwrite").saveAsTable(ENRICHED_TABLE)
    log.info("Enriched written → %s (total %d)", ENRICHED_TABLE, enriched_count)

    # --- Gold (persisted read) ---
    enriched_persisted = spark.table(ENRICHED_TABLE)
    gold_df = enriched_persisted.filter(trim(lower(col("student_status"))) == "current")
    gold_count = gold_df.count()
    gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
    log.info("Gold written → %s (total %d current)", GOLD_TABLE, gold_count)

    log.info("SUMMARY | enriched=%d | gold=%d", enriched_count, gold_count)
    display(spark.table(GOLD_TABLE).limit(10))

# COMMAND ----------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Workflow failed: %s", e)
        raise
