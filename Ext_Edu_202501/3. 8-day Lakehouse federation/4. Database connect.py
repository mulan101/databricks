# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC - db-1-instance-1.cpcg84ycur1x.ap-northeast-2.rds.amazonaws.com
# MAGIC - user - edu
# MAGIC - DB - edu250113
# MAGIC - PW - Dd98969321$9

# COMMAND ----------

!pip install aws-advanced-python-wrapper
!pip install psycopg
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## psycopg library 를 이용한 연결 

# COMMAND ----------

from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
        Connection.connect,
        "host=db-1-instance-1.cpcg84ycur1x.ap-northeast-2.rds.amazonaws.com dbname=edu250113 user=edu password=Dd98969321$9",
        wrapper_dialect="aurora-pg",
        autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT * FROM pg_catalog.pg_tables where 1=1 and schemaname = 'public'")
    awscursor.fetchone()
    for record in awscursor:
        print(record)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Pyspark 를 이용한 연결

# COMMAND ----------

username = "edu"
password = "Dd98969321$9"
driver = "org.postgresql.Driver"

hostname = "db-1-instance-1.cpcg84ycur1x.ap-northeast-2.rds.amazonaws.com"
port = "5432"
database = "edu250113"

jdbc_url = f"jdbc:postgresql://{hostname}:{port}/{database}"

print(jdbc_url)

query = "SELECT * FROM pg_catalog.pg_tables where 1=1 and schemaname = 'public'"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", query) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

# COMMAND ----------

display(df)
