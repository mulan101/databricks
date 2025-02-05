# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## External location 생성
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > Catalog Explorer
# MAGIC >> External Data
# MAGIC >>> Create external location
# MAGIC
# MAGIC ![](files/image/externel_01.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC > Catalog Explorer
# MAGIC >> External Data
# MAGIC >>> Create external location
# MAGIC >>>> Manual
# MAGIC
# MAGIC ![](files/image/external_02.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC > Catalog Explorer
# MAGIC >> External Data
# MAGIC >>> Create external location
# MAGIC >>>> Manual
# MAGIC
# MAGIC - External location name
# MAGIC - URL
# MAGIC - Storage credential
# MAGIC
# MAGIC ![](files/image/external_03.png)
# MAGIC
# MAGIC
# MAGIC - External location name : 임의 지정 `ex) ddbx_bucket_name_storage`
# MAGIC - URL : `s3://{bucket_name}/`
# MAGIC - Storage credential
# MAGIC
# MAGIC
# MAGIC 💾 role 생성 후에 이어서 생성

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create role

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > IAM
# MAGIC >> roles
# MAGIC >>> Create role
# MAGIC
# MAGIC - Create role 1-1
# MAGIC ![](files/image/external_04.png)
# MAGIC
# MAGIC
# MAGIC - Create role 1-2
# MAGIC
# MAGIC ![](files/image/external_05.png)
# MAGIC
# MAGIC - Create role 1-3
# MAGIC
# MAGIC ![](files/image/external_06.png)
# MAGIC
# MAGIC - Create role 1-4
# MAGIC
# MAGIC ![](files/image/external_07.png)
# MAGIC
# MAGIC - Create role 1-5
# MAGIC
# MAGIC ![](files/image/external_09.png)
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create storage credential

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC - 생성한 role ARN 을 복사합니다.
# MAGIC
# MAGIC ![](files/image/cre_role_01.png)
# MAGIC
# MAGIC - ㅁㄴㅇ
# MAGIC
# MAGIC ![](files/image/cre_role_02.png)
# MAGIC
# MAGIC - asd 
# MAGIC
# MAGIC ![](files/image/cre_role_03.png)
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Update role

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC update role 1-1
# MAGIC
# MAGIC ![](files/image/up_role_01.png)
# MAGIC
# MAGIC
# MAGIC update role 1-2
# MAGIC
# MAGIC ![](files/image/up_role_02.png)
# MAGIC
# MAGIC
# MAGIC update role 1-3
# MAGIC
# MAGIC - Assume role 에 자기 자신 추가 
# MAGIC - Databricks Account consol External ID 추가 : 90502a8f-a874-4b2b-af62-cba114ffc420
# MAGIC
# MAGIC ![](files/image/up_role_05.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC > Catalog Explorer
# MAGIC >> External Data
# MAGIC >>> Create external location
# MAGIC >>>> Manual
# MAGIC
# MAGIC - External location name
# MAGIC - URL
# MAGIC - Storage credential
# MAGIC
# MAGIC ![](files/image/external_03.png)
# MAGIC
# MAGIC
# MAGIC - External location name : 임의 지정 `ex) ddbx_bucket_name_storage`
# MAGIC - URL : `s3://{bucket_name}/`
# MAGIC - Storage credential
# MAGIC
# MAGIC
# MAGIC 💾
# MAGIC
# MAGIC ![](files/image/external_10.png)
# MAGIC
# MAGIC - 생성 확인
# MAGIC
# MAGIC ![](files/image/external_12.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC External location이 정상적으로 동작하는지 확인하기위해 다음을 실행합니다. 

# COMMAND ----------


my_bucket = ''

dbutils.fs.cp('s3://ddbx-academy/usage-data/online_retail.csv', f's3://{my_bucket}/online_retail.csv')

# COMMAND ----------

dbutils.fs.ls(f's3://{my_bucket}/')
