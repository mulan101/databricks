# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="files/image/DD_07.png" alt="Databricks Learning" style="width: 1500px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 단축키
# MAGIC
# MAGIC Help > Keyboard shortcuts (H)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 자주 쓰는 단축키
# MAGIC
# MAGIC * a: 선택된 Cell 이전에 추가(above)
# MAGIC * b: 선택된 Cell 다음에 추가(below)
# MAGIC * ctrl+enter: 선택된 Cell 실행
# MAGIC * shift+enter: 선택된 Cell 실행 후 다음 Cell 이동
# MAGIC * 위, 아래 방향키: Cell 이동
# MAGIC * enter: 편집 모드로 변경
# MAGIC * esc: 명령 모드로 변경
# MAGIC * d+d: 선택된 Cell 삭제
# MAGIC * shift+d+d: prompt 없이 Cell 삭제
# MAGIC * l: line number toggle
# MAGIC * t: Cell 제목 toggle

# COMMAND ----------

# d+d 를 눌러서 삭제

# COMMAND ----------

# shift+d+d 를 눌러서 삭제

# COMMAND ----------

# MAGIC %md
# MAGIC ## Magic Command

# COMMAND ----------

# MAGIC %md
# MAGIC ### %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata

# COMMAND ----------

# MAGIC %md
# MAGIC ### %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs

# COMMAND ----------

# MAGIC %md
# MAGIC ### %md

# COMMAND ----------

# MAGIC %md
# MAGIC markdown 가능
# MAGIC
# MAGIC * ul1
# MAGIC * ul2
# MAGIC   * ul21
# MAGIC   * ul22
# MAGIC * ul3
# MAGIC
# MAGIC 1. ol1
# MAGIC 1. ol2
# MAGIC    1. ol21
# MAGIC    1. ol22
# MAGIC 1. ol3
# MAGIC
# MAGIC Table
# MAGIC
# MAGIC |Column1|Column2|
# MAGIC |:---|:---:|
# MAGIC |Value11|Value12|
# MAGIC |Value21|Value22|
# MAGIC |Value31|Value32|
# MAGIC
# MAGIC Code
# MAGIC
# MAGIC ```shell
# MAGIC cd <TEST_DIR>
# MAGIC ls -al <TEST_DIR>
# MAGIC ```
# MAGIC
# MAGIC `#`, `##`, `###`, `####`, `#####`, `######` 을 이용하여 Header 작성시 노트북 좌측 Table of contents 를 눌러서 목차를 확인할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 더 자세한 notebook code 문서는 [여기](https://docs.databricks.com/notebooks/notebooks-code.html)에서 확인하실 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 지원언어

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

# DBTITLE 1,%py 와 %python 가능
# MAGIC %py
# MAGIC a = 1
# MAGIC b = 2
# MAGIC c = a + b
# MAGIC c

# COMMAND ----------

# DBTITLE 1,Variable Explorer
c

# COMMAND ----------

# DBTITLE 1,Notebook default language: Python
import os
os.listdir('/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scala

# COMMAND ----------

# DBTITLE 1,공용 클러스터(UC)의 경우 불가능했었으나 Scala Preview 지원되어 사용 가능
# MAGIC %scala
# MAGIC val a = 1
# MAGIC val b = 2
# MAGIC val c = a + b
# MAGIC c

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select a + b as c
# MAGIC from (
# MAGIC   select 1 as a, 2 as b
# MAGIC ) t

# COMMAND ----------

# MAGIC %md
# MAGIC ### R

# COMMAND ----------

# DBTITLE 1,Access mode 가 Shared 인 경우 불가능
# MAGIC %r
# MAGIC a <- 1
# MAGIC b <- 2
# MAGIC c <- a + b
# MAGIC c

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils

# COMMAND ----------

# MAGIC %md
# MAGIC ### fs

# COMMAND ----------

display(dbutils.fs)

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Hadoop hdfs API 가 익숙하시다면 아래 FileSystem class 를 사용하여 dbfs 를 사용할 수 있습니다.
# MAGIC dbutils.fs.dbfs

# COMMAND ----------

# MAGIC %md
# MAGIC 자세한 file system 문서는 [여기](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs)에서 확인하실 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### widgets

# COMMAND ----------

display(dbutils.widgets)

# COMMAND ----------

dbutils.widgets.text('text1', '텍스트', 'text')

# COMMAND ----------

dbutils.widgets.combobox('combobox1', '값2', ['값1', '값2', '값3'], 'Combobox')

# COMMAND ----------

dbutils.widgets.combobox('dropdown1', 'value3', ['value1', 'value2', 'value3'], 'Dropdown')

# COMMAND ----------

dbutils.widgets.multiselect('multiselect1', 'value1', ['value1', 'value2', 'value3'], 'Multiselect')

# COMMAND ----------

# label 로 정렬되어 widget 이 추가됩니다.

# COMMAND ----------

dbutils.widgets.get('text1')

# COMMAND ----------

dbutils.widgets.get('combobox1')

# COMMAND ----------

dbutils.widgets.get('dropdown1')

# COMMAND ----------

dbutils.widgets.get('multiselect1')

# COMMAND ----------

dbutils.widgets.remove('text1')

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 자세한 widget 문서는 [여기](https://docs.databricks.com/notebooks/widgets.html)에서 확인하실 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
