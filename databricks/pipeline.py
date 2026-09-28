# ============================================================
# OLIST PIPELINE ORCHESTRATION
# ============================================================

# Run Bronze
dbutils.notebook.run(
    "./01_bronze_ingestion",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

# Run Silver
dbutils.notebook.run(
    "./02_silver_processing",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

# Run Gold
dbutils.notebook.run(
    "./03_gold",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

print("Olist pipeline completed successfully.")