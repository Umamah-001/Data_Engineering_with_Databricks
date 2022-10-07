# Databricks notebook source
# MAGIC %md  
# MAGIC # Create and Manage Clusters
# MAGIC 
# MAGIC A Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning. You run these workloads as a set of commands in a notebook or as an automated job. 
# MAGIC 
# MAGIC Databricks makes a distinction between all-purpose clusters and job clusters. 
# MAGIC * You use all-purpose clusters to analyze data collaboratively using interactive notebooks.
# MAGIC * You use job clusters to run fast and robust automated jobs.
# MAGIC 
# MAGIC This demo will cover creating and managing all-purpose Databricks clusters using the Databricks Data Science & Engineering Workspace. 

# COMMAND ----------

# MAGIC %md  
# MAGIC # Create Cluster
# MAGIC 
# MAGIC Depending on the workspace in which you're currently working, you may or may not have cluster creation privileges. Instructions in this section assume that you do have cluster creation privileges.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Use the left sidebar to navigate to the **Compute** page by clicking on the ![compute](https://files.training.databricks.com/images/clusters-icon.png) icon
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC 1. Give a name of your choice in the **Cluster name**.
# MAGIC 1. Set the **Cluster mode** to **Single Node** or **Multi Node**.
# MAGIC 1. Select a **Databricks runtime version**.
# MAGIC 1. Leave boxes checked for the default settings under the **Autopilot Options**
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC 
# MAGIC **NOTE:** Clusters can take several minutes to deploy. Once you have finished deploying a cluster, feel free to continue to explore the cluster creation UI.

# COMMAND ----------

# MAGIC %md
# MAGIC # Manage Clusters
# MAGIC 
# MAGIC Once the cluster is created, go back to the **Compute** page to view the cluster. Select a cluster to review the current configuration. 
# MAGIC 
# MAGIC Click the **Edit** button. Note that most settings can be modified (if you have sufficient permissions). Changing most settings will require running clusters to be restarted.

# COMMAND ----------

# MAGIC %md
# MAGIC # Restart, Terminate, and Delete
# MAGIC 
# MAGIC Note that while **Restart**, **Terminate**, and **Delete** have different effects, they all start with a cluster termination event. (Clusters will also terminate automatically due to inactivity assuming this setting is used.)
# MAGIC 
# MAGIC When a cluster terminates, all cloud resources currently in use are deleted. This means:
# MAGIC * Associated VMs and operational memory will be purged
# MAGIC * Attached volume storage will be deleted
# MAGIC * Network connections between nodes will be removed
# MAGIC 
# MAGIC In short, all resources previously associated with the compute environment will be completely removed. This means that **any results that need to be persisted should be saved to a permanent location**. Note that you will not lose your code, nor will you lose data files that you've saved out appropriately.
# MAGIC 
# MAGIC The **Restart** button will allow us to manually restart our cluster. This can be useful if we need to completely clear out the cache on the cluster or wish to completely reset our compute environment.
# MAGIC 
# MAGIC The **Terminate** button allows us to stop our cluster. We maintain our cluster configuration setting, and can use the **Restart** button to deploy a new set of cloud resources using the same configuration.
# MAGIC 
# MAGIC The **Delete** button will stop our cluster and remove the cluster configuration.