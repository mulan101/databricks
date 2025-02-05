# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 과제 시나리오 
# MAGIC
# MAGIC ### `아래의 요구사항에 맞도록 코드를 작성하고 실행합니다. `
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Using Methods 
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations: **`select`**, **`where`**, **`orderBy`**, **`filter`**, **`withColumn`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> actions: **`select`**, **`count`**, **`take`**, **`drop`**
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %py
# MAGIC import re
# MAGIC
# MAGIC # TODO catalog_name 변경
# MAGIC catalog_name = 'edu2501'
# MAGIC username = spark.sql("SELECT current_user()").first()[0]
# MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
# MAGIC
# MAGIC print(f'catalog_name = {catalog_name}')
# MAGIC print(f'username = {username}')
# MAGIC print(f'clean_username = {clean_username}')
# MAGIC
# MAGIC spark.sql(f'set edu.catalog={catalog_name}')
# MAGIC spark.sql(f"set edu.username={username}")
# MAGIC spark.sql(f"set edu.clean_username={clean_username}")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ${edu.catalog}.${edu.clean_username};
# MAGIC use catalog ${edu.catalog};
# MAGIC use ${edu.catalog}.${edu.clean_username};

# COMMAND ----------


source_df = spark.read.format("csv").option("header", "true").load("s3://ddbx-academy/usage-data/online_retail.csv")

source_df.write.mode("overwrite").saveAsTable("online_retail")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 1. SparkSession 을 사용해서 테이블을 DataFrame 으로 생성해주세요.
# MAGIC
# MAGIC - table :  **`{catalog}.{user_schema}.online_retail`**
# MAGIC - DataFrame : df

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 2. DataFrame과 DataFrame 의 Schema 를 출력해 주세요.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 3. a.조건으로 필터링하고 b.조건으로 데이터를 정렬해 새로운 DataFrame 을 생성해주세요.
# MAGIC
# MAGIC - a. column : Country = "France"
# MAGIC - b. column : InvoiceDate 오름차순 정렬
# MAGIC - DataFrame : filter_df

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 4. 데이터의 전체 conut 와 처음 5개 행을 출력해 주세요.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 5. filter_df 에 현재 시간을 나타내는 컬럼을 추가해 주세요.
# MAGIC
# MAGIC - column name : dttm
# MAGIC - winthColumn() 메서드 사용
# MAGIC - curunt_time() 메서드 사용
