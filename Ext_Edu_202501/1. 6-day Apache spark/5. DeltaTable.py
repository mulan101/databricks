# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Managing Delta Tables

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

# MAGIC %md
# MAGIC ## Delta Table 생성
# MAGIC
# MAGIC Delta Lake 테이블을 생성하는 방법은 여러가지가 있습니다. 가장 쉬운 방법으로, CREATE TABLE 구문을 이용하여 빈 테이블을 생성해 봅시다.
# MAGIC
# MAGIC **NOTE:** Databricks Runtime 8.0 이상의 버전에서는 Delta Lake 가 디폴트 포맷이므로 **`USING DELTA`** 를 생략할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS students (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   value DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table에 데이터 입력
# MAGIC
# MAGIC 일반적으로 테이블에 데이터를 입력할 때는 다른 데이터셋 쿼리 결과를 이용하여 입력하는 경우가 많습니다. 
# MAGIC
# MAGIC 하지만 아래와 같이 INSERT 구문을 사용하여 직접 입력하는 것도 가능합니다.  

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 위의 셀에서는 3건의 **`INSERT`** 구문을 각각 실행했습니다. 각각의 구문은 ACID가 보장된 별개의 트랜잭션으로 처리됩니다. 
# MAGIC
# MAGIC 아래와 같이 한 번의 트랜잭션에 여러 레코드를 입력할 수 있습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO students
# MAGIC VALUES 
# MAGIC   (4, "Ted", 4.7),
# MAGIC   (5, "Tiffany", 5.5),
# MAGIC   (6, "Vini", 6.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table 조회

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM students
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md <i18n value="4ecaf351-d4a4-4803-8990-5864995287a4"/>
# MAGIC
# MAGIC Delta Lake 는 **항상** 해당 테이블의 가장 최신 버전 데이터를 읽어 오며, 진행중인 다른 작업에 영향 받지 않습니다. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 레코드 UPDATE
# MAGIC
# MAGIC Delta Lake를 사용하면 마치 Database를 사용하는 것처럼 insert, update, delete를 사용해서 손쉽게 데이터셋을 수정할 수 있습니다. UPDATE 작업도 ACID 트랜잭션이 보장됩니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE students 
# MAGIC SET value = value + 1
# MAGIC WHERE name LIKE "T%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM students
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 레코드 DELETE
# MAGIC
# MAGIC DELETE 작업도 역시 ACID 트랜잭션이 보장됩니다. 즉, 데이터의 일부만 삭제되어 일관성이 깨지는 것을 걱정할 필요가 없습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM students 
# MAGIC WHERE value > 6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM students
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE 를 이용한 Upsert 
# MAGIC
# MAGIC Databricks에서는 **MERGE** 문을 이용하여 upsert (데이터의 Update, Insert 및 기타 데이터 조작을 하나의 명령어로 수행)를 처리할 수 있습니다. 
# MAGIC
# MAGIC 아래의 예제는 운영계 DB 테이블의 변경사항을 데이터레이크의 테이블에 반영하기 위해, CDC(Change Data Capture) 로그 데이터를 데이터레이크에 적재한 후 임시 View로 읽어들였다고 가정합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
# MAGIC   (2, "Omar", 15.2, "update"),
# MAGIC   (3, "", null, "delete"),
# MAGIC   (7, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");
# MAGIC   
# MAGIC SELECT * FROM updates;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 이 view에는 레코드들에 대한 3가지 타입- Insert, Update, Delete 명령어 기록을 담고 있습니다.  
# MAGIC 이 명령어를 각각 수행한다면 3개의 트랜잭션이 되고 만일 이중에 하나라도 실패하게 된다면 invalid한 상태가 될 수 있습니다.  
# MAGIC 대신에 이 3가지 action을 하나의 atomic 트랜잭션으로 묶어서 한꺼번에 적용되도록 합니다.  
# MAGIC <br>
# MAGIC **`MERGE`**  문은 최소한 하나의 기준 field (여기서는 id)를 가지며, 각 **`WHEN MATCHED`** 이나 **`WHEN NOT MATCHED`**  구절은 여러 조건값들을 가질 수 있습니다.  
# MAGIC **id** 필드를 기준으로 **type** 필드값에 따라서 각 record에 대해서 update, delete, insert문을 수행하게 됩니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO students b
# MAGIC USING updates u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM students
# MAGIC ORDER BY id
