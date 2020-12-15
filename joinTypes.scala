// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/816254878439192/2363209404289369/6894355921072367/latest.html


val data1=sc.parallelize(List( ("Amandeep",2020), ("Singh",2010) ))
val data2=sc.parallelize(List( ("Manpreet","Singh"), ("Singh","Punjab") ))

// COMMAND ----------

data1.join(data2).collect()

// COMMAND ----------

data1.leftOuterJoin(data2).collect()

// COMMAND ----------

data1.rightOuterJoin(data2).collect()

// COMMAND ----------

data1.fullOuterJoin(data2).collect()
