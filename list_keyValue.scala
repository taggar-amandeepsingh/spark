// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/816254878439192/1166594940938312/6894355921072367/latest.html

val listData= List("Aman 1998", "Manpreet 1999", "Hardik 2000", "Raman 2001")

// COMMAND ----------

val listRDD=sc.parallelize(listData)
val RDDdata = listRDD.map( x=> ( x.split(" ")(0), x.split(" ")(1).toInt ))

// COMMAND ----------

RDDdata.mapValues(x =>x+10).collect()
