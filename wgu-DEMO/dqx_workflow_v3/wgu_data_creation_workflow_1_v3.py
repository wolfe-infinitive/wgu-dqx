# Databricks notebook source
# MAGIC %skip
# MAGIC %pip install faker pyyaml pandas delta-spark databricks-labs-dqx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# 🧩 WGU DQX Workflow • Synthetic Data Generator (Bronze Layer)
# Documentation: https://github.com/databricks-labs/dqx
#
# Purpose:
# Generates synthetic student data and upserts into the Bronze Delta table.
# Configurable via:
# /Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_workflow_config.yaml

import os, string, random, yaml, logging, pandas as pd, pytz
from datetime import datetime, timedelta
from faker import Faker
from typing import Dict, Any
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Structured logging
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# COMMAND ----------

def load_config() -> Dict[str, Any]:
    """Load YAML configuration and validate required sections."""
    config_path = "/Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_workflow_config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    if "table_names" not in cfg or "bronze" not in cfg["table_names"]:
        raise KeyError("Missing required config key: table_names['bronze']")
    log.info("✅ Loaded configuration from: %s", config_path)
    return cfg

# COMMAND ----------

def get_spark() -> SparkSession:
    """Initialize or reuse an active Spark session."""
    return SparkSession.builder.getOrCreate()

# COMMAND ----------

def upsert_by_key(spark: SparkSession, df, table: str, key: str):
    """Performs a Delta merge (upsert) based on a key column."""
    count = df.count()
    if spark.catalog.tableExists(table):
        delta = DeltaTable.forName(spark, table)
        (
            delta.alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("Upserted %d records into existing table: %s", count, table)
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table)
        log.info("Created new table %s with %d records.", table, count)

# COMMAND ----------

def generate_student_id() -> str:
    """Generate a random student ID (three letters + eight digits)."""
    return "".join(random.choices(string.ascii_lowercase, k=3)) + f"{random.randint(0, 99999999):08d}"

# COMMAND ----------

def generate_synthetic_students(n: int = 100, clean_fraction: float = 0.15):
    """Generate synthetic student records with controlled data noise."""
    fake = Faker("en_US")
    random.seed(datetime.now().timestamp())
    start_date = datetime(2024, 1, 1)
    now = datetime.now(pytz.utc)

    rows = []
    clean_cutoff = int(n * clean_fraction)

    for i in range(n):
        sid = generate_student_id()
        name = fake.name()
        email = f"{name.lower().replace(' ', '_')}@gmail.com"
        status = random.choice(["graduated", "current", "break", "dropped"])

        record = dict(
            student_id=sid,
            name=name,
            email=email,
            student_status=status,
            month_end_date=start_date + timedelta(days=random.randint(0, 365)),
            paid=random.choice([True, False]),
            stays_on_campus=random.choice([True, False]),
            load_timestamp=now,
        )

        # Introduce noise into ~85% of rows
        if i >= clean_cutoff:
            if random.random() < 0.4:
                record["student_id"] = "INVALID_" + record["student_id"]
            if random.random() < 0.4:
                record["email"] = record["email"].replace("@gmail", "@fake")
            if random.random() < 0.2:
                record["name"] = "123" + record["name"]

        rows.append(record)

    return pd.DataFrame(rows)

# COMMAND ----------

def main():
    """Main synthetic data generation workflow."""
    cfg = load_config()
    BRONZE_TABLE = cfg["table_names"]["bronze"]

    log.info("Target Bronze Table: %s", BRONZE_TABLE)

    spark = get_spark()
    pdf = generate_synthetic_students(n=100, clean_fraction=0.15)
    df = spark.createDataFrame(pdf)

    upsert_by_key(spark, df, BRONZE_TABLE, "student_id")

    log.info("✅ Synthetic Bronze dataset successfully upserted → %s", BRONZE_TABLE)
    display(df.limit(10))

# COMMAND ----------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Synthetic data generation failed: %s", e)
        raise
