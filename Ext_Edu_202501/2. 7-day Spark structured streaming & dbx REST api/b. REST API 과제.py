# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 학습 목표
# MAGIC
# MAGIC - Databricks REST API 를 호출하고 데이터를 정재하여 테이블을 생성합니다. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 요구사항 
# MAGIC
# MAGIC 1. 클러스터 목록을 조회하는 <a href="https://docs.databricks.com/api/workspace/clusters/list">REST API</a> 로 테이블을 생성해 주세요.
# MAGIC
# MAGIC 2. 테이블 정의
# MAGIC
# MAGIC #### Table Name :  edu2501.{user_schema}.cluster_log_info 
# MAGIC
# MAGIC ||creator_user_name|cluster_name|spark_version|node_type_id|driver_node_type_id|ResourceClass|autotermination_minutes|etl_ymdh|
# MAGIC |--|--|--|--|--|--|--|--|--|
# MAGIC |type|STRING|STRING|STRING|STRING|STRING|STRING|INT|TIMESTAMP|
# MAGIC |commnet|생성자|클러스터 이름|스파크 버전|워커 타입|마스터 타입|리소스 타입|자동 종료 시간(min)|수집시간|
# MAGIC |example data|inqyu.park@data-dynamics.io|park ingyu's Cluster|16.1.x-scala2.12|m5d.large|m5d.large|SingleNode|30|2025-01-22T10:00:10|

# COMMAND ----------

import requests
import json

token = ""

url = "https://ddbx-academy.cloud.databricks.com/api/2.1/clusters/list"
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

response = requests.get(url, headers=headers)
result = response.json()


