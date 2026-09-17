# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# DBTITLE 1,Provision this student's catalog + medallion schemas
catalog = resolve_catalog()
create_schemas(catalog, SCHEMAS)
spark.sql(f"USE CATALOG `{catalog}`")

# Provision the raw inputs (idempotent): generate the synthetic telemetry Bronze
# tables and upload the real UBC open geodata to a Volume for Lab 02 to ingest.
GEODATA_DIR = ensure_bronze_data(catalog)

# Fully-qualified schema names the lab notebook can reference.
DA_CATALOG = catalog
DA_BRONZE = f"{catalog}.{BRONZE_SCHEMA}"
DA_SILVER = f"{catalog}.{SILVER_SCHEMA}"
DA_GOLD = f"{catalog}.{GOLD_SCHEMA}"
DA_CONFIG = f"{catalog}.{CONFIG_SCHEMA}"     # holds the Genie space id for Lab 06

# COMMAND ----------

display_config_values([
    ('Catalog', catalog, True),
    ('Bronze schema', DA_BRONZE, True),
    ('Silver schema', DA_SILVER, True),
    ('Gold schema', DA_GOLD, True),
    ('Geodata volume', GEODATA_DIR, GEODATA_DIR),
    ('Workshop scale', f'{WORKSHOP_SCALE} &nbsp;(fraction of the full reference dataset)'),
])
setup_complete_msg()
