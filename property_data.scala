// Databricks notebook source
// /FileStore/tables/Property_data.csv

val propData=sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeHeader = propData.filter( x=> !x.contains("Price"))

// COMMAND ----------

val room= removeHeader.map( x=> (x.split(",")(3).toInt, (1, x.split(",")(2).toDouble) ))

// COMMAND ----------

val reducedRDD=room.reduceByKey( (x,y) => (x._1+y._1 , x._2+y._2) )

// COMMAND ----------

val averagePrice=reducedRDD.mapValues( x => x._2/x._1)

// COMMAND ----------

for( (bedroom , avg) <- averagePrice.collect() ) println(bedroom+": "+avg)
