-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL UDFs and Control Flow

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import re
-- MAGIC
-- MAGIC # TODO catalog_name 변경
-- MAGIC catalog_name = 'edu2501'
-- MAGIC username = spark.sql("SELECT current_user()").first()[0]
-- MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
-- MAGIC
-- MAGIC print(f'catalog_name = {catalog_name}')
-- MAGIC print(f'username = {username}')
-- MAGIC print(f'clean_username = {clean_username}')
-- MAGIC
-- MAGIC spark.sql(f'set edu.catalog={catalog_name}')
-- MAGIC spark.sql(f"set edu.username={username}")
-- MAGIC spark.sql(f"set edu.clean_username={clean_username}")

-- COMMAND ----------

create schema if not exists ${edu.catalog}.${edu.clean_username};
use catalog ${edu.catalog};
use ${edu.catalog}.${edu.clean_username};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Simple Dataset

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW foods(food) AS VALUES
("beef"),
("beans"),
("potatoes"),
("bread");

SELECT * FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL UDFs

-- COMMAND ----------

CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")

-- COMMAND ----------

SELECT yelling(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scoping and Permissions of SQL UDFs

-- COMMAND ----------

DESCRIBE FUNCTION yelling

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CASE/WHEN

-- COMMAND ----------

SELECT *,
  CASE 
    WHEN food = "beans" THEN "I love beans"
    WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
    WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
    ELSE concat("I don't eat ", food)
  END
FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simple Control Flow Functions

-- COMMAND ----------

CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;

-- COMMAND ----------

SELECT foods_i_like(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## More Details
-- MAGIC * https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html
-- MAGIC * https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-function.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup

-- COMMAND ----------

DROP VIEW IF EXISTS foods;
DROP FUNCTION IF EXISTS yelling;
DROP FUNCTION IF EXISTS foods_i_like;

-- COMMAND ----------

drop schema if exists ${edu.clean_username} cascade;

-- COMMAND ----------

drop schema if exists `${edu.username}` cascade;

-- COMMAND ----------


