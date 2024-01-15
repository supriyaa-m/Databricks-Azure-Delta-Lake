# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.san17.dfs.core.windows.net","SAS")

# COMMAND ----------

spark.conf.set("fs.azure.sas.token.provider.type.san17.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# COMMAND ----------

spark.conf.set("fs.azure.sas.fixed.token.san17.dfs.core.windows.net",
               "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-14T14:01:15Z&st=2024-01-14T06:01:15Z&spr=https&sig=7Dg3PevLVf%2B4mdUWq4nYgJhWHIpn2xLjT2r1unMQr1k%3D")

# COMMAND ----------

df = spark.read.csv("abfs://inputdatasets@san17.dfs.core.windows.net/orders-230714-200740.csv",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("parquet").save("abfs://inputdatasets@san17.dfs.core.windows.net/parquet/orders.parquet")

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("delta").save("abfs://inputdatasets@san17.dfs.core.windows.net/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists retaildb

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.ordersparquet using parquet location "abfs://inputdatasets@san17.dfs.core.windows.net/parquet/orders.parquet"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersparquet

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table retaildb.ordersparquet

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.ordersparquet using parquet location "abfs://inputdatasets@san17.dfs.core.windows.net/parquet/orders.parquet/*"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersparquet

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.ordersdelta using delta location "abfs://inputdatasets@san17.dfs.core.windows.net/delta/orders.delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retaildb.ordersdelta

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("delta").option("path","abfs://inputdatasets@san17.dfs.core.windows.net/delta/orders.delta").saveAsTable("retaildb.ordersdeltatable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersdeltatable

# COMMAND ----------



# COMMAND ----------


