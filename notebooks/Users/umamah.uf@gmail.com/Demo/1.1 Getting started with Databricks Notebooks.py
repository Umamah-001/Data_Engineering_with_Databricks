# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Basics
# MAGIC A notebook is a web-based interface to a document that contains runnable code, visualizations, and narrative text. They provide the means of developing and executing code interactively on Databricks. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Attach to a Cluster
# MAGIC Directly below the name of the notebook at the top of your screen, use the drop-down list to connect this notebook to your cluster.
# MAGIC 
# MAGIC **NOTE**: Deploying a cluster can take several minutes. A green arrow will appear to the right of the cluster name once resources have been deployed. If your cluster has a solid gray circle to the left, you will need to start the cluster before executing your notebook code.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Notebooks Basics
# MAGIC 
# MAGIC Notebooks provide cell-by-cell execution of code. Multiple languages can be mixed in a notebook. Users can add plots, images, and markdown text to enhance their code readibility.
# MAGIC 
# MAGIC ## Run a Cell
# MAGIC * Run the cell below using one of the following options:
# MAGIC   1. **CTRL+ENTER** or **CTRL+RETURN**
# MAGIC   2. **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
# MAGIC   3. Using **Run Cell**, **Run All Above** or **Run All Below** as seen here<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>
# MAGIC   
# MAGIC **NOTE**: Cell-by-cell code execution means that cells can be executed multiple times or out of order.

# COMMAND ----------

print("I'm running my first command!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Setting the Default Notebook Language
# MAGIC 
# MAGIC The above cell executes a Python command because our default language for the notebook is set to Python.
# MAGIC 
# MAGIC Databricks notebooks support Python, SQL, Scala, and R. A language can be selected when a notebook is created, but this can be changed at any time.
# MAGIC 
# MAGIC The default language appears directly to the right of the notebook title at the top of the page. 
# MAGIC We'll change the default language for this notebook to SQL.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click on the **Python** next to the notebook title at the top of the screen
# MAGIC 2. In the UI that pops up, select **SQL** from the drop down list 
# MAGIC 
# MAGIC **NOTE**: In the cell just before this one, you should see a new line appear with <strong><code>&#37;python</code></strong>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create and Run a SQL Cell
# MAGIC 1. To run an SQL cell in a notebook whose default language is not SQL, you need to add the %sql magic command.
# MAGIC 2. Run the below cell to execute an sql command

# COMMAND ----------

# MAGIC %sql
# MAGIC select "I am running an SQL command"

# COMMAND ----------

# MAGIC %md
# MAGIC # Magic Commands
# MAGIC 1. Magic commands are specific to the Databricks notebooks
# MAGIC 2. These are built-in commands that provide the same outcome regardless of the notebook's language
# MAGIC 3. A single percent (%) symbol at the start of a cell identifies a magic command
# MAGIC   * You can only have one magic command per cell
# MAGIC   * A magic command must be the first thing in a cell

# COMMAND ----------

# MAGIC %md
# MAGIC # Language Magics
# MAGIC Language magic commands allow for the execution of code in languages other than the notebook's default. 
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC 
# MAGIC Adding the language magic for the currently set notebook type is not necessary.
# MAGIC 
# MAGIC When we changed the notebook language from Python to SQL above, existing cells written in Python had the <strong><code>&#37;python</code></strong> command added.
# MAGIC 
# MAGIC **NOTE**: Rather than changing the default language of a notebook constantly, you should stick with a primary language as the default and only use language magics as necessary to execute code in another language.

# COMMAND ----------

print("Python")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "SQL"

# COMMAND ----------

# MAGIC %md 
# MAGIC # Markdown
# MAGIC 
# MAGIC The magic command **&percnt;md** allows us to render Markdown in a cell:
# MAGIC * Double click this cell to begin editing it
# MAGIC * Then hit **`Esc`** to stop editing
# MAGIC 
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC 
# MAGIC 
# MAGIC **Bold Text**
# MAGIC *Italicized Text*
# MAGIC 
# MAGIC This is an ordered list
# MAGIC 1. one
# MAGIC 1. two
# MAGIC 1. three
# MAGIC 
# MAGIC This is an unordered list
# MAGIC * A
# MAGIC * B
# MAGIC * C

# COMMAND ----------

# MAGIC %md 
# MAGIC # %run
# MAGIC 1. You can run a notebook from another notebook by using the magic command **%run**
# MAGIC 2. Notebooks to be run are specified with relative paths
# MAGIC 3. The referenced notebook executes as if it were part of the current notebook, so all variables, views and other local declarations will be available from the calling notebook

# COMMAND ----------

# MAGIC %md
# MAGIC We can call **display** command on a dataframe to render its output in a tabular format.
# MAGIC The **display()** command has the following capabilities and limitations:
# MAGIC * Preview of results limited to 1000 records
# MAGIC * Provides button to download results data as csv
# MAGIC * can render the output as plots (selection of plots is available)

# COMMAND ----------

# MAGIC %md
# MAGIC # Downloading Notebooks
# MAGIC There are a number of options for downloading either individual notebooks or collections of notebooks.
# MAGIC Steps:
# MAGIC 1. Click the **File** option inthe right corner at the top of the notebook
# MAGIC 2. From the menu, hover over **Export** and then select **Source File**
# MAGIC 
# MAGIC The notebook will download to your personal laptop. It will be named with the current notebook name and have the file extension for the default language. You can open this notebook with any file editor and see the raw contents of Databricks notebooks.
# MAGIC 
# MAGIC Additionally, you can upload these downloaded files in any Databricks workspace by using the **Import** option.