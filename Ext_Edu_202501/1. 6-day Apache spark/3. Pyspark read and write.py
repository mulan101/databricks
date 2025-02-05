# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 학습목표 
# MAGIC
# MAGIC - Pyspark 에서 데이터 read, write 하는 방법을 알아봅니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # read
# MAGIC
# MAGIC ---
# MAGIC - 파일을 대상으로 데이터를 읽어올경우 사용합니다.
# MAGIC - DataFrame 을 리턴합니다.
# MAGIC - 기본 문법 
# MAGIC - spark.read
# MAGIC ---

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### read format

# COMMAND ----------

.foramt("jdbc")
.format("delta")
.format("parquet")
.format("csv")
.format("text")
.format("json")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### read options

# COMMAND ----------

.option("delimiter", ",") # 구분자 지정 / csv 파일일 경우 
.option("header", "true") # 첫 line 이 schema 인지 지정 / csv 파일일 경우
.option("encoding", "euc-kr") # 인코딩
.option("inferSchema", "true") # Schema 추정 옵션 / 반정형 데이터인 경우
.option("fetchsize", "10000") # 한번에 읽어올 size 지정 / JDBC 를 사용할 경우
.option("recursiveFileLookup","true") # 지정된 path 하위 파일 모두 읽기

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 테이블 대상으로 데이터를 읽어올 경우
# MAGIC
# MAGIC ---
# MAGIC - spark.sql()
# MAGIC - spark.table()
# MAGIC - DataFrame 을 리턴합니다.
# MAGIC ---

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### sql( )

# COMMAND ----------

query = "SELECT * FROM {catalog}.{schema}.{table_name}"
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### table( )

# COMMAND ----------

table_name = "{catalog}.{schema}.{table_name}"
df = spark.table(table_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ---
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # write
# MAGIC
# MAGIC ---
# MAGIC - DataFrame.write
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### write format 

# COMMAND ----------

.foramt("jdbc")
.format("delta")
.format("parquet")
.format("csv")
.format("text")
.format("json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### write mode( )

# COMMAND ----------

.mode("append")
.mode("overwite")
.mode("error")
.mode("ignore")
