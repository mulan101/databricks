-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Tutorial: Work with Spark DataFrames on Databricks
-- MAGIC DataFrame 은 2차원 데이터 구조입니다. 엑셀, SQL Table 과 비슷합니다. 다양한 데이터 분석 문제를 해결할 수 있는 기능들을 제공합니다.
-- MAGIC
-- MAGIC DataFrame 은 RDD(Resilient Distributed Datasets) 기반의 추상화입니다. Spark DataFrame 과 Spark SQL 은 통합 계획 및 최적화 엔진을 사용하여 다양한 언어에서 동일한 얻을 낼 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Python

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]
-- MAGIC
-- MAGIC pdf = pd.DataFrame(data, columns=["id", "name"])
-- MAGIC
-- MAGIC df1 = spark.createDataFrame(pdf)
-- MAGIC df2 = spark.createDataFrame(data, schema="id LONG, name STRING")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC case class Employee(id: Int, name: String)
-- MAGIC
-- MAGIC val df = Seq(new Employee(1, "Elia"), new Employee(2, "Teo"), new Employee(3, "Fang")).toDF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## R
-- MAGIC Shared mode 의 경우에는 불가능합니다.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC # Load the SparkR package that is already preinstalled on the cluster.
-- MAGIC library(SparkR)
-- MAGIC # Get the existing SparkSession that is already initiated on the cluster.
-- MAGIC sparkR.session()
-- MAGIC
-- MAGIC # Construct a list of sample data. You must use a type suffix
-- MAGIC # for the integers here or you might get the error
-- MAGIC # "java.lang.Double is not a valid external type for schema of int."
-- MAGIC data   <- list(
-- MAGIC             list(1L, "Raymond"),
-- MAGIC             list(2L, "Loretta"),
-- MAGIC             list(3L, "Wayne")
-- MAGIC           )
-- MAGIC
-- MAGIC # Specify the data's schema (the columns' names and data types).
-- MAGIC schema <- structType(
-- MAGIC             structField("id",   "integer"),
-- MAGIC             structField("name", "string")
-- MAGIC           )
-- MAGIC
-- MAGIC # Create the SparkDataFrame based on the specified data and schema.
-- MAGIC df     <- createDataFrame(
-- MAGIC             data   = data,
-- MAGIC             schema = schema
-- MAGIC           )
-- MAGIC
-- MAGIC # Print the SparkDataFrame's schema.
-- MAGIC printSchema(df)
-- MAGIC
-- MAGIC # Print the SparkDataFrame's contents.
-- MAGIC showDF(df)
-- MAGIC
-- MAGIC # Output:
-- MAGIC #
-- MAGIC # root
-- MAGIC #  |-- id: integer (nullable = true)
-- MAGIC #  |-- name: string (nullable = true)
-- MAGIC # +---+-------+
-- MAGIC # | id|   name|
-- MAGIC # +---+-------+
-- MAGIC # |  1|Raymond|
-- MAGIC # |  2|Loretta|
-- MAGIC # |  3|  Wayne|
-- MAGIC # +---+-------+

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extracting Data Directly from Files
-- MAGIC
-- MAGIC Databricks 에서 Spark SQL 을 사용하여 파일로부터 data 를 직접 추출합니다.
-- MAGIC
-- MAGIC 지원되는 file format 은 다음과 같습니다.
-- MAGIC
-- MAGIC * delta
-- MAGIC * parquet
-- MAGIC * orc
-- MAGIC * json
-- MAGIC * csv
-- MAGIC * avro
-- MAGIC * text
-- MAGIC * binaryFile

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

-- Schema 생성
create schema if not exists ${edu.catalog}.${edu.clean_username};
use catalog ${edu.catalog};
use ${edu.catalog}.${edu.clean_username};

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Catalog Exploer 에서 확인 가능합니다. 
-- MAGIC
-- MAGIC ![](files/image/ce_01.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Overview
-- MAGIC JSON 으로 쓰여진 iot stream device data 로 진행해봅니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = 'dbfs:/databricks-datasets/iot-stream/data-device'
-- MAGIC print(dataset_path)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC zcat /dbfs/databricks-datasets/iot-stream/data-device/part-00000.json.gz | head -2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 현재는 경로가 `dbfs:` 로 시작하지만, 실제 프로젝트에서는 외부 cloud storage 에서 읽어야 합니다.
-- MAGIC
-- MAGIC Account Admin, Metastore Admin, Workspace Admin 권한을 가진 User 가 이런 cloud storage 접근을 제어할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query a Single File
-- MAGIC
-- MAGIC 파일 1개에 포함된 데이터를 쿼리하려면 아래 패턴으로 쿼리를 실행합니다.
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC 경로에는 <code>&#x60;</code>(백틱)을 사용해야 합니다. '(작은따옴표)가 아닙니다.

-- COMMAND ----------

SELECT *
FROM json.`dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz`
LIMIT 10

-- COMMAND ----------

SELECT count(*)
FROM json.`dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query a Directory of Files
-- MAGIC
-- MAGIC directory 내부 모든 파일이 같은 format 과 같은 schema 를 가진 경우, directory 를 지정하여 전체 파일을 쿼리할 수 있습니다.

-- COMMAND ----------

SELECT *
FROM json.`dbfs:/databricks-datasets/iot-stream/data-device/`
LIMIT 10

-- COMMAND ----------

SELECT count(*)
FROM json.`dbfs:/databricks-datasets/iot-stream/data-device/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create References to Files
-- MAGIC 현재는 temporary view 로 생성했지만 영구적인 view 를 생성할 수 있습니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW datasets_temp_view
AS SELECT * FROM json.`dbfs:/databricks-datasets/iot-stream/data-device/`;

SELECT *
FROM datasets_temp_view
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract Text Files as Raw Strings
-- MAGIC
-- MAGIC `text` 를 사용하여 실제 값을 `value` 라는 column 으로 읽을 수 있습니다.

-- COMMAND ----------

SELECT *
FROM text.`dbfs:/databricks-datasets/iot-stream/data-device/`
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract the Raw Bytes and Metadata of a File
-- MAGIC
-- MAGIC image 나 unstructured data 를 처리할 때는 전체 파일로 작업해야 할 수 있습니다.
-- MAGIC
-- MAGIC `binaryFile` 을 사용하여 directory 를 쿼리하면 파일의 이진 표현과 metadata(`path`, `modificationTime`, `length`, `content`)를 확인할 수 있습니다.

-- COMMAND ----------

SELECT *
FROM binaryFile.`dbfs:/databricks-datasets/iot-stream/data-device/`;

-- COMMAND ----------

SELECT *
FROM binaryFile.`dbfs:/FileStore/image/DD_07.png`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /databricks-datasets/iot-stream/data-user

-- COMMAND ----------

SELECT *
FROM csv.`/databricks-datasets/iot-stream/data-user`

-- COMMAND ----------

CREATE OR REPLACE TABLE users_csv
AS
SELECT *
FROM read_files(
  'dbfs:/databricks-datasets/iot-stream/data-user',
  format => 'csv',
  header => 'true',
  delimiter => ',' 
)

-- COMMAND ----------

DESCRIBE EXTENDED users_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parquet

-- COMMAND ----------

SELECT *
FROM parquet.`/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet`
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta

-- COMMAND ----------

SELECT *
FROM delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
LIMIT 10

-- COMMAND ----------

drop table if exists users_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
