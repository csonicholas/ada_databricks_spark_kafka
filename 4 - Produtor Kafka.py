# Databricks notebook source
# MAGIC %md 
# MAGIC # 4 - Criação de mensagem

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1. Instalação de dependências

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install kafka-python

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Preparando ambiente

# COMMAND ----------

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=["localhost:9092"]) #declara o produtor


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Produzindo 

# COMMAND ----------

topico = 'termos'
termo = 'cancer'.encode('utf-8')
producer.send(topico, termo)

# COMMAND ----------

import pyspark.pandas as ps

local_streaming = '/FileStore/tables/projeto/artigos_streaming.parquet'

df = ps.read_parquet(local_streaming)
df.criterio_relevancia.value_counts()

# COMMAND ----------

# dbutils.fs.rm(local_streaming, True)

# COMMAND ----------

