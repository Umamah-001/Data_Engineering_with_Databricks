# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrating Jobs with Databricks Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC # Create and configure a Workflow for Databricks Notebooks
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar.
# MAGIC 2. Select the **Jobs** tab.
# MAGIC 3. Click on the **Create Job** button.
# MAGIC 4. Enter a name in **Task Name**.
# MAGIC 5. Select **Notebook** in **Type**.
# MAGIC 6. In **Path**, add the path of your notebook,
# MAGIC 7. Select an existing all-purpose cluster or create a new job cluster.
# MAGIC 8. Add specific parameters if needed in key-value pairs.
# MAGIC 9. In **Advanced Options**, add any dependent libaries the cluster needs to install before executing the notebook. Also set notifications and retry policy.
# MAGIC 10. In the top-left of the screen, rename the job (not the task) from **`Reset`** (the defaulted value) to the **Job Name** value provided in the cell above.
# MAGIC 11. Finally, click **Create**.
# MAGIC 12. Click the blue **Run now** button in the top right to start the job.

# COMMAND ----------

# MAGIC %md
# MAGIC # Scheduling of Databricks Jobs
# MAGIC 1. On the right hand side of the Jobs UI under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 2. Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 3. Changing the **Schedule type** field from **Manual** to **Scheduled** will bring up a cron scheduling UI. Select a scheduling time of your choice.

# COMMAND ----------

# MAGIC %md
# MAGIC # Review Job Run
# MAGIC 1. Select the **Job runs** tab in the **Workflows** section.
# MAGIC 1. Find your job. If the job is still running, it will be under the Active runs section. If the job finished running, it will be under the Completed runs section
# MAGIC 1. Open the Output details by clicking on the timestamp field under the Start time column
# MAGIC 1. If the job is still running, you will see the active state of the notebook with a Status of **Pending** or **Running** in the right side panel. If the job has completed, you will see the full execution of the notebook with a status of **Succeeded** or **Failed** in the right side panel