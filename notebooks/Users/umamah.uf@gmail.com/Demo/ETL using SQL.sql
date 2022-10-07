-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create a Delta Table

-- COMMAND ----------

CREATE TABLE employee
  (id INT, employee_name STRING, salary DOUBLE);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS employee
  (id INT, employee_name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Insert Data

-- COMMAND ----------

INSERT INTO employee VALUES (1, "Sarah", 1000);
INSERT INTO employee VALUES (2, "John", 1200);
INSERT INTO employee VALUES (3, "Mark", 1500);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Query a Delta Table

-- COMMAND ----------

SELECT * FROM employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Update existing Records

-- COMMAND ----------

UPDATE employee 
SET salary = salary + 500
WHERE employee_name ="Sarah"

-- COMMAND ----------

SELECT * FROM employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Delete Records

-- COMMAND ----------

DELETE FROM employee
WHERE employee_name ="John"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Using Merge

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW update_employees(id, employee_name, salary, type) AS VALUES
  (2, "John", 1300, "insert"),
  (3, "Mark", 1500, "delete"),
  (4, "Hannah", 1250, "insert"),
  (1, "Sarah", 2000, "update");
  
SELECT * FROM update_employees;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fe009d5-513f-4b93-994f-1ae9a0f30a80"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Using the syntax we've seen so far, we could filter from this view by type to write 3 statements, one each to insert, update, and delete records. But this would result in 3 separate transactions; if any of these transactions were to fail, it might leave our data in an invalid state.
-- MAGIC 
-- MAGIC Instead, we combine these actions into a single atomic transaction, applying all 3 types of changes together.
-- MAGIC 
-- MAGIC **`MERGE`** statements must have at least one field to match on, and each **`WHEN MATCHED`** or **`WHEN NOT MATCHED`** clause can have any number of additional conditional statements.
-- MAGIC 
-- MAGIC Here, we match on our **`id`** field and then filter on the **`type`** field to appropriately update, delete, or insert our records.

-- COMMAND ----------

MERGE INTO employee a
USING update_employees u
ON a.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Drop Table

-- COMMAND ----------

DROP TABLE employee

-- COMMAND ----------

