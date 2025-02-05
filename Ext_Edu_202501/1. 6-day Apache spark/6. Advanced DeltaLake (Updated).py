# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Delta Lake Features
# MAGIC
# MAGIC Delta Lake 테이블이 제공하는 고유한 기능들을 살펴 보겠습니다.  
# MAGIC
# MAGIC * **`OPTIMIZE`** 구문을 이용하여 자잘한 파일들을 적절한 크기로 최적화 (compaction) 
# MAGIC * **`ZORDER`**  구문을 이용한 테이블 인덱싱
# MAGIC * Delta Lake 테이블의 디렉토리 구조 살펴보기
# MAGIC * 테이블 트랜잭션 이력 조회
# MAGIC * 테이블의 이전 버전 데이터를 조회하거나 해당 버전으로 롤백
# MAGIC * **`VACUUM`** 을 이용하여 오래된 데이터 정리 
# MAGIC
# MAGIC **참조문서**
# MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
# MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

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
# MAGIC ## 테이블 정보 살펴보기 
# MAGIC
# MAGIC database, table, view 에 대한 정보는 디폴트로 Hive metastore 에 저장됩니다. 
# MAGIC
# MAGIC **`DESCRIBE EXTENDED`** 구문을 이용하여 테이블의 metadata 를 살펴 봅시다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended students

# COMMAND ----------

# MAGIC %md
# MAGIC 다른 방법으로, **`DESCRIBE DETAIL`** 구문을 이용하여 테이블 metadata 를 조회할 수 있습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL students

# COMMAND ----------

# MAGIC %md 
# MAGIC **`Location`** 필드에서 해당 테이블을 구성하는 파일들의 저장 경로를 확인할 수 있습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE students

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake 파일들 살펴보기 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow

# COMMAND ----------

# MAGIC %md
# MAGIC 테이블 저장 경로 디렉토리에는 parquet 포맷의 데이터 파일들과 **`_delta_log`** 디렉토리가 있습니다.  
# MAGIC
# MAGIC Delta Lake 테이블의 레코드들은 parquet 파일로 저장됩니다. 
# MAGIC
# MAGIC Delta Lake 테이블의 트랜잭션 기록들은 **`_delta_log`** 디렉토리 아래에 저장됩니다. 이 디렉토리를 좀 더 살펴 봅시다.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC 각각의 트랜잭션 로그들은 버전별 JSON 파일로 저장됩니다. 여기서는 8개의 트랜잭션 로그 파일을 볼 수 있습니다. (버전은 0부터 시작합니다) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데이터 파일 살펴보기
# MAGIC
# MAGIC 이전에 살펴봤던 nyctaxi_yellow 테이블은 많은 수의 데이터 파일로 이루어져 있습니다. 
# MAGIC
# MAGIC **`DESCRIBE DETAIL`** 구문으로 파일 갯수(numFiles)를 살펴 봅시다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC metadata 조회 결과, nyctaxi_yellow 테이블의 현재 버전에는 188개의 데이터 파일이 있습니다 (numFiles=188). 그럼 테이블 디렉토리에 있는 다른 parquet 파일들은 무엇일까요?
# MAGIC
# MAGIC Delta Lake는 변경된 데이터를 담고 있는 파일들을 overwrite 하거나 즉시 삭제하지 않고, 해당 버전에서 유효한 데이터 파일들을 트랜잭션 로그에 기록합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM json.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/_delta_log/00000000000000000001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC **`add`** 컬럼에는 테이블에 새롭게 추가된 파일들의 정보가 담겨 있고, **`remove`** 컬럼에는 이 트랜잭션으로 삭제 처리된 파일들이 표시되어 있습니다.
# MAGIC
# MAGIC Delta Lake 테이블을 쿼리할 때, 쿼리엔진은 이 트랜잭션 로그를 이용하여 현재 버전에서 유효한 파일들을 알아내고 그 외의 데이터 파일들은 무시합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 작은 파일들의 최적화 (Compaction) 과 인덱싱
# MAGIC
# MAGIC 이런저런 작업을 하다 보면 작은 데이터 파일들이 많이 생성되는 경우가 종종 발생합니다. 이와 같이 작은 파일들이 많은 경우 처리 성능 저하의 원인이 될 수 있습니다.
# MAGIC
# MAGIC **`OPTIMIZE`** 명령어는 기존의 데이터 파일내의 레코드들을 합쳐서 새로 최적의 사이즈로 파일을 만들고, 기존의 작은 파일들을 읽기 성능이 좋은 큰 파일들로 대체합니다.
# MAGIC
# MAGIC 이 때 하나 이상의 필드를 지정해서 **`ZORDER`** 인덱싱을 함께 수행할 수 있습니다.
# MAGIC
# MAGIC Z-Ordering은 관련 정보를 동일한 파일 집합에 배치하여, 쿼리 실행시 읽어야 하는 데이터의 양을 줄여 성능을 향상 시키는 기술입니다. 쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 ZORDER BY를 사용하면 효과적입니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE students
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Liquid Clustering
# MAGIC 전통적인 테이블 파티셔닝과 ZORDER 방법을 대체하여 데이터 레이아웃 결정을 단순화하고 쿼리 성능을 최적화합니다. 주요 특징은 다음과 같습니다:
# MAGIC
# MAGIC   * 유연성: 기존 데이터를 다시 쓰지 않고 클러스터링 키를 재정의할 수 있습니다.
# MAGIC
# MAGIC   * 성능: 쿼리 패턴에 따라 데이터 레이아웃을 최적화하여 쿼리 성능을 향상시킵니다.
# MAGIC
# MAGIC   * 호환성: Partitioning 또는 ZORDER와 호환되지 않습니다.  
# MAGIC   
# MAGIC
# MAGIC 사용 사례: 고유 값이 많은 열로 자주 필터링되는 테이블, 데이터 분포에 큰 편차가 있는 테이블, 빠르게 성장하는 테이블에 유용합니다.\
# MAGIC 기존 테이블에 liquid clustering을 활성화하거나 테이블 생성 시 CLUSTER BY 구문을 추가하여 사용할 수 있습니다.
# MAGIC - [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
# MAGIC ## Partition
# MAGIC
# MAGIC 큰 테이블을 더 작은 부분으로 나누어 관리하고 쿼리 성능을 향상시키는 방법입니다. 파티셔닝을 통해 데이터 액세스 속도를 높이고, 관리의 용이성을 제공하며, 시스템의 성능을 최적화할 수 있습니다.
# MAGIC
# MAGIC Databricks에서 사용되는 주요 파티셔닝 유형은 다음과 같습니다:
# MAGIC
# MAGIC  * 범위 파티셔닝: 특정 범위의 값을 기준으로 데이터를 나눕니다. 예를 들어, 날짜 범위에 따라 데이터를 나눌 수 있습니다.
# MAGIC
# MAGIC  * 리스트 파티셔닝: 특정 값 목록을 기준으로 데이터를 나눕니다. 예를 들어, 국가 코드에 따라 데이터를 나눌 수 있습니다.
# MAGIC
# MAGIC  * 해시 파티셔닝: 해시 함수를 사용하여 데이터를 균등하게 나눕니다.
# MAGIC
# MAGIC  * 조합 파티셔닝: 위의 여러 파티셔닝 방법을 조합하여 사용합니다.
# MAGIC
# MAGIC 파티셔닝을 통해 데이터베이스는 더 효율적으로 쿼리를 처리하고, 데이터 관리의 복잡성을 줄일 수 있습니다.
# MAGIC - [Partition](https://docs.databricks.com/en/sql/language-manual/sql-ref-partition.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel
# MAGIC
# MAGIC Delta Lake 테이블의 모든 변경 이력은 트랜잭션 로그에 저장되므로, **`DESCRIBE HISTORY`** 구문을 통해 <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a> 를 쉽게 조회할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY edu2501.inqyu_park_data_dynamics_io.cluster_log_info

# COMMAND ----------

# MAGIC %md
# MAGIC auto optimize 로 인해 새로운 버전(version 9)이 추가되어 students 테이블의 최신 버전이 된 것을 볼 수 있습니다.
# MAGIC
# MAGIC 트랜잭션 로그에서 removed 로 마킹된 예전 데이터 파일들이 삭제되지 않고 남아 있으므로, 이를 이용하여 테이블의 과거 버전 데이터를 조회할 수 있습니다. 아래와 같이 버전 번호나 timestamp를 지정하여 Time travel을 수행할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC -- FROM edu2501.inqyu_park_data_dynamics_io.cluster_log_info VERSION AS OF 3
# MAGIC -- SELECT * FROM edu2501.inqyu_park_data_dynamics_io.cluster_log_info@v3 order by userId;
# MAGIC -- SELECT * FROM edu2501.inqyu_park_data_dynamics_io.cluster_log_info TIMESTAMP AS OF '2023-02-01 00:05:00';

# COMMAND ----------

# MAGIC %md
# MAGIC Time travel 은 현재 버전에서 트랜잭션을 undo 하거나 과거 상태의 데이터를 다시 생성하는 방식이 아니라, 트랜잭션 로그를 이용하여 해당 버전에서 유효한 파일들을 찾아낸 후, 이들을 쿼리하는 방식으로 이루어 집니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 과거 버전으로 Rollback 하기

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 실수로 모든 레코드를 삭제한 상황을 가정해 봅시다.
# MAGIC DELETE FROM students

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# MAGIC %md
# MAGIC 삭제 커밋이 수행되기 이전 버전으로 rollback 해 봅시다.

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE students TO VERSION AS OF 9

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM students
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC 테이블 히스토리를 조회(DESCRIBE HISTORY)해 보면 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">RESTORE</a> 명령이 트랜잭션으로 기록됨을 볼 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stale File 정리하기
# MAGIC
# MAGIC Delta Lake의 Versioning과 Time Travel은 과거 버전을 조회하고 실수했을 경우 데이터를 rollback하는 매우 유용한 기능이지만, 데이터 파일의 모든 버전을 영구적으로 저장하는 것은 스토리지 비용이 많이 들게 됩니다. 또한 개인정보 관련 규정에 따라 데이터를 명시적으로 삭제해야 할 경우도 있습니다. 
# MAGIC
# MAGIC **`VACUUM`** 을 이용하여 Delta Lake Table 에서 불필요한 데이터 파일들을 정리할 수 있습니다. 
# MAGIC
# MAGIC **`VACUUM`** 명령을 수행하면 지정한 retention 기간 이전으로 더이상 여행할 수 없게 됩니다.  

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM students RETAIN 1 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC 기본값으로 VACUUM 은 7일 미만의 데이터를 삭제하지 못하도록 설정되어 있으며, 이는 아직 사용중이거나 커밋되지 않은 파일이 삭제되어 데이터가 손상되는 것을 방지하기 위함입니다. 
# MAGIC
# MAGIC 아래의 예제는 이 기본 설정을 무시하고 가장 최근 버전 데이터만 남기고 모든 과거 버전의 stale file을 정리하는 예제입니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC
# MAGIC -- DRY RUN 을 이용하여 VACUUM 실행시 삭제될 파일들을 미리 파악할 수 있습니다.   
# MAGIC VACUUM students RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM 을 실제 실행합니다.
# MAGIC VACUUM students RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC 테이블 디렉토리에서 데이터 파일들이 정상적으로 삭제되었는지 확인해 봅시다.
# MAGIC
# MAGIC UC 환경인 경우 권한 없음으로 실패할 수 있습니다.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls s3://db-911c8f3589ae73417852cb8e0f6ba999-s3-root-bucket/41a1fe84-2dd2-4f1c-b6c8-e2489ba8f724/tables/7d747c48-a436-4da2-bbed-812c925af9fc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 테이블 CLONE
# MAGIC
# MAGIC * `DEEP CLONE`: data 와 metadata 모두 복제하고, incremental 복제하므로 다시 실행하면 변경분을 동기화할 수 있지만, 대용량의 경우 오래 걸릴 수 있습니다.
# MAGIC * `SHALLOW CLONE`: 현재 테이블 수정 없이 data 는 복제하지 않고, delta transaction log 만 복제합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE <TARGET_DEEP_CLONE_TABLE_NAME>
# MAGIC -- DEEP CLONE <SOURCE_TABLE_NAME>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE <TARGET_SHALLOW_CLONE_TABLE_NAME>
# MAGIC -- SHALLOW CLONE <SOURCE_TABLE_NAME>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 테이블 DROP
# MAGIC
# MAGIC **`DROP TABLE`** 구문을 이용하여 테이블을 삭제할 수 있습니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS students

# COMMAND ----------

# MAGIC %md
# MAGIC ##  DESCRIBE EXTENDED, DESCRIBE DETAIL, DESCRIBE HISTORTY 비교
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC describe extended edu2501.inqyu_park_data_dynamics_io.cluster_log_info;
# MAGIC -- column detail, table info
# MAGIC
# MAGIC describe DETAIL edu2501.inqyu_park_data_dynamics_io.cluster_log_info;
# MAGIC -- table properties, table features
# MAGIC
# MAGIC describe history edu2501.inqyu_park_data_dynamics_io.cluster_log_info; 
# MAGIC -- 테이블 버전 정보 , operation (create, write)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
