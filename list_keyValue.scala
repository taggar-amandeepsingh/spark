// Databricks notebook source
val listData= List("Aman 1998", "Manpreet 1999", "Hardik 2000", "Raman 2001")

// COMMAND ----------

val listRDD=sc.parallelize(listData)
val RDDdata = listRDD.map( x=> ( x.split(" ")(0), x.split(" ")(1).toInt ))

// COMMAND ----------

RDDdata.mapValues(x =>x+10).collect()
