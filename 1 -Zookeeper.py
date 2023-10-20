# Databricks notebook source
# MAGIC %sh 
# MAGIC sudo wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz

# COMMAND ----------

# MAGIC %sh 
# MAGIC tar -xvf kafka_2.12-3.5.1.tgz

# COMMAND ----------

# MAGIC %sh 
# MAGIC ./kafka_2.12-3.5.1/bin/zookeeper-server-start.sh ./kafka_2.12-3.5.1/config/zookeeper.properties

# COMMAND ----------

