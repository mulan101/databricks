# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakehouse Federation
# MAGIC
# MAGIC
# MAGIC ### 학습 목표 
# MAGIC
# MAGIC - External table에 대해 알아보고 table 을 생성합니다. 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 특징
# MAGIC
# MAGIC - Databricks는 외부 테이블의 메타데이터만 관리합니다. 
# MAGIC - Unity Catalog의 테이블 등록은 데이터 파일에 대한 포인터일 뿐입니다. 외부 테이블을 삭제하면 데이터 파일은 삭제되지 않습니다.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 지원하는 파일 형식
# MAGIC
# MAGIC - DELTA
# MAGIC - CSV
# MAGIC - JSON
# MAGIC - AVRO
# MAGIC - PARQUET
# MAGIC - ORC
# MAGIC - TEXT

# COMMAND ----------

import re

# TODO catalog_name 변경
catalog_name = 'edu2501'
username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

print(f'catalog_name = {catalog_name}')
print(f'username = {username}')
print(f'clean_username = {clean_username}')

spark.sql(f'set edu.catalog={catalog_name}')
spark.sql(f"set edu.username={username}")
spark.sql(f"set edu.clean_username={clean_username}")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${edu.catalog};
# MAGIC use ${edu.catalog}.${edu.clean_username};

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 새로운 경로에 테이블 생성

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${edu.catalog}.${edu.clean_username}.ext_table_online_retail
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ("headers" = "true")
# MAGIC LOCATION 's3://{my_bucket}/ext_tbl/online_retail'
# MAGIC AS SELECT * FROM csv.`s3://{my_bucket}/online_retail.csv`

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 기존 파일 경로 사용

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${edu.catalog}.${edu.clean_username}.ext_table_online_retail2
# MAGIC USING CSV
# MAGIC TBLPROPERTIES ("headers" = "true")
# MAGIC LOCATION 's3://{my_bucket}/online_retail.csv'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Table Drop 이후 파일 삭제 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE ${edu.catalog}.${edu.clean_username}.ext_table_online_retail2
