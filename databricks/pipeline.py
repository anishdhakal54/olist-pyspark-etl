# ============================================================
# OLIST PIPELINE ORCHESTRATION
# ============================================================

# Run Bronze
dbutils.notebook.run(
    "./bronze",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

# Run Silver
dbutils.notebook.run(
    "./silver",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

# Run Gold
dbutils.notebook.run(
    "./gold",
    0,
    {
        "storage_account": dbutils.widgets.get("storage_account"),
        "container": dbutils.widgets.get("container")
    }
)

print("Olist pipeline completed successfully.")