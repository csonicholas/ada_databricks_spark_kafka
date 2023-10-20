# Databricks notebook source
# MAGIC %sh
# MAGIC ./kafka_2.12-3.5.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic termos --partitions 1 --replication-factor 1

# COMMAND ----------

# MAGIC %sh
# MAGIC ./kafka_2.12-3.5.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic termos

# COMMAND ----------

# MAGIC %sh
# MAGIC ./kafka_2.12-3.5.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic termos --from-beginning

# COMMAND ----------

