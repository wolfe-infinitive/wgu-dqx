# Databricks notebook source
# MAGIC %skip
# MAGIC %pip install databricks-labs-dqx pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# 🧩 WGU Ad-Hoc DQX Profiler (Config-Driven)
# Docs: https://databrickslabs.github.io/dqx/docs/guide/data_profiling/
#
# Purpose:
#   Fully configuration-driven profiling workflow. Takes a YAML config specifying
#   input sources (table or file), profiling parameters, and output destinations.
#
# Supports:
#   • Single-table profiling
#   • File-based sources (Delta, Parquet, CSV, JSON)
#   • Multi-table profiling via config list
#   • Persisting rule outputs (YAML + optional Delta table)
#
# Example config: /Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_adhoc_profiler_config.yaml

# COMMAND ----------

import os, yaml, logging
from datetime import datetime
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator

# Structured logging setup
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("DQX_ConfigProfiler")

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# COMMAND ----------

def load_config():
    """Load and validate the configuration file."""
    default_path = "/Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_adhoc_jobs/dqx_adhoc_profiler_config.yaml"
    config_path = os.getenv("DQX_CONFIG_PATH", default_path)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Config file not found: {config_path}")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    if "input_sources" not in cfg:
        raise KeyError("❌ Config missing required section: 'input_sources'")
    if "profiling" not in cfg:
        raise KeyError("❌ Config missing required section: 'profiling'")

    log.info("✅ Loaded configuration from %s", config_path)
    return cfg

# COMMAND ----------

def load_input_source(source_cfg):
    """Return Spark DataFrame for given input source config."""
    src_type = source_cfg.get("type", "table").lower()
    fmt = source_cfg.get("format", "delta")
    opts = source_cfg.get("options", {})
    path = source_cfg.get("table_name") or source_cfg.get("path")

    if not path:
        raise ValueError("Missing table_name or path for input source.")

    if src_type == "table":
        log.info("📘 Loading table source: %s", path)
        return spark.table(path), path
    elif src_type == "file":
        log.info("📂 Loading file source: %s (format=%s)", path, fmt)
        return spark.read.format(fmt).options(**opts).load(path), path
    else:
        raise ValueError(f"Unsupported source type: {src_type}")

# COMMAND ----------

def run_profiler_for_source(df, source_name, profiling_cfg, output_cfg):
    """Run DQX Profiler and save auto-generated rules."""
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    os.makedirs(output_cfg["directory"], exist_ok=True)

    log.info("🧩 Profiling started for: %s", source_name)
    summary_stats, profiles = profiler.profile(df, options=profiling_cfg)
    auto_rules = generator.generate_dq_rules(profiles)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rule_yaml_path = os.path.join(output_cfg["directory"], f"{source_name}_rules_{timestamp}.yaml")

    with open(rule_yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(auto_rules, f, sort_keys=False)

    log.info("✅ Generated %d rules → %s", len(auto_rules), rule_yaml_path)

    if output_cfg.get("rule_table"):
        rule_df = spark.createDataFrame(
            [(r.get("name"), r["check"]["function"], r["criticality"]) for r in auto_rules],
            ["rule_name", "function", "criticality"]
        )
        rule_df.write.format("delta").mode("overwrite").saveAsTable(output_cfg["rule_table"])
        log.info("💾 Persisted rules to Delta table: %s", output_cfg["rule_table"])

# COMMAND ----------

def main():
    cfg = load_config()

    input_sources = cfg["input_sources"]
    profiling_cfg = cfg["profiling"]
    output_cfg = cfg.get("rule_output", {
        "directory": "/Workspace/Shared/wgu-dqx/wgu-DEMO/dqx_workflow_v3/dqx_adhoc_jobs/adhoc_profiler_results/"
    })

    # Normalize structure (allow single or multiple inputs)
    sources = (
        input_sources["multi_table"]
        if "multi_table" in input_sources
        else [input_sources]
        if isinstance(input_sources, dict)
        else input_sources
    )

    log.info("=" * 80)
    log.info("🚀 DQX Config-Driven Ad-Hoc Profiler")
    log.info("Profiling %d source(s)", len(sources))
    log.info("Output Directory: %s", output_cfg["directory"])
    log.info("=" * 80)

    for src in sources:
        df, src_name = load_input_source(src)
        run_profiler_for_source(df, src.get("name", src_name.split(".")[-1]), profiling_cfg, output_cfg)

    log.info("=" * 80)
    log.info("✅ All profiling complete — rules saved to %s", output_cfg["directory"])
    log.info("=" * 80)

# COMMAND ----------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("❌ Profiler failed: %s", e)
        raise
