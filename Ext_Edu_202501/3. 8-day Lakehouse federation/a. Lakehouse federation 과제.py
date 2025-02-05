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
# MAGIC ### `아래의 요구사항에 맞도록 프로그래밍 하고 워크플로우를 생성합니다. `
# MAGIC
# MAGIC - 한국 관광안내소의 데이터를 수집하고 특정 기준에 맞게 score를 생성합니다.
# MAGIC
# MAGIC ❗ 각 항목별로 notebook 을 생성해 주세요.
# MAGIC
# MAGIC 1. Bronze table 을 생성합니다.
# MAGIC 2. dimension table 을 생성합니다.
# MAGIC 3. silver table 을 생성합니다. 
# MAGIC 4. gold table 을 생성합니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 1. Bronze table 을 생성합니다.
# MAGIC   - 배치로 데이터를 수집하여 Bronze table 을 생성합니다.
# MAGIC   - source file : s3://ddbx-academy/usage-data/korea_tour/20250122/
# MAGIC   - External table 로 생성해 주세요.
# MAGIC   - External location : s3://{my_bucket}/ext_tbl/
# MAGIC
# MAGIC |Type|Source Path|수집간격|mode|
# MAGIC |---|---|---|---|
# MAGIC |batch|s3://ddbx-academy/usage-data/korea_tour/{yyyyMMdd}/|매일 07시 00분 실행|append|
# MAGIC
# MAGIC - etl_ymdh 컬럼을 추가해 주세요 (timestamp type)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 2. Dimension table 을 생성합니다. 
# MAGIC   - 배치로 데이터를 수집하여 dimension table 을 생성합니다. 
# MAGIC   - s3://ddbx-academy/usage-data/korea_tour/dim/
# MAGIC   - Managed table 로 생성해 주세요.
# MAGIC
# MAGIC   |Type|Source Path|수집간격|mode|
# MAGIC |---|---|---|---|
# MAGIC |batch|s3://ddbx-academy/usage-data/korea_tour/dim/|매일 01시 00분 실행|overwrite|
# MAGIC
# MAGIC - etl_ymdh 컬럼을 추가해 주세요 (timestamp type)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 3. Silver table 을 생성합니다. 
# MAGIC   - bronze table 에서 해당일 데이터만 조회 후 적재해주세요.
# MAGIC   - bronze table 과 dimension table 을 조인합니다.
# MAGIC     - join 조건 : seq_num  
# MAGIC   - score 컬럼을 추가하고 value 를 생성해주세요.
# MAGIC     - value 생성조건 : 안내 가능 언어 1개당 + 1점, 연중무휴 +1점
# MAGIC
# MAGIC     - ex)
# MAGIC       |영어|일본어|중국어|휴무일|score|
# MAGIC       |---|---|---|---|---|
# MAGIC       |Y|Y|Y|설 당일+추석 당일|3|
# MAGIC       |N|NULL|Y|NULL|1|
# MAGIC       |N|N|Y|연중무휴|2|
# MAGIC
# MAGIC   - 데이터를 filtering 합니다. 
# MAGIC     - filter column : 시도명
# MAGIC     - filtering 데이터로 silver table 을 생성해 주세요.
# MAGIC
# MAGIC       ```
# MAGIC       경상남도 - Gyeongsangnam-do
# MAGIC       경기도 - Gyeonggi-do
# MAGIC       전라남도 - Jeollanam-do
# MAGIC       대전광역시 - Daejeon
# MAGIC       강원도 - Gangwon-do
# MAGIC       강원특별자치도 - Gangwon-Special
# MAGIC       충청남도 - Chungcheongnam-do
# MAGIC       충청북도 - Chungcheongbuk-do
# MAGIC       부산광역시 - Busan
# MAGIC       울산광역시 - Ulsan
# MAGIC       서울특별시 - Seoul
# MAGIC       대구광역시 - Daegu
# MAGIC       광주광역시 - Gwangju
# MAGIC       제주특별자치도 - Jeju-Special
# MAGIC       경상북도 - Gyeongsangbuk-do
# MAGIC       전라북도 - Jeollabuk-do
# MAGIC       인천광역시 - Incheon
# MAGIC
# MAGIC       ```
# MAGIC     - 테이블명 예시 경상남도  :  **`edu2501.{user_schema}.korea_tuor_gyeongsangnam_do_silver`**
# MAGIC
# MAGIC
# MAGIC |Type|Source Path|수집간격|mode|
# MAGIC |---|---|---|---|
# MAGIC |batch|bronze table|매일 01시 00분 실행|append|
# MAGIC
# MAGIC ❗️ bronze task Depends
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 4. Gold table
# MAGIC
# MAGIC - silver table 에서 해당일 데이터만 조회 후 적재해주세요.
# MAGIC - 데이터를 pivoting 합니다.
# MAGIC   - pivot column : 관광안내소명
# MAGIC   - value : score
# MAGIC   - agg : first
# MAGIC
# MAGIC - pivoting column(관광안내소명) 에 commnet를 추가해주세요.
# MAGIC   - comment : 안내소소개
# MAGIC
# MAGIC - 테이블명 예시 경상남도  :  **`edu2501.{user_schema}.korea_tuor_gyeongsangnam_do_gold`**
# MAGIC
# MAGIC |Type|Source Path|수집간격|mode|
# MAGIC |---|---|---|---|
# MAGIC |batch|silver table|매일 01시 00분 실행|append|
# MAGIC
# MAGIC ❗️ silver task Depends
