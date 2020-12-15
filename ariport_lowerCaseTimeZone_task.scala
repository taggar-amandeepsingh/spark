// Databricks notebook source
// /FileStore/tables/airports.text

val airportData = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val dataRDD= airportData.map (x=> ( x.split(",")(3), x.split(",")(11).toLowerCase() ))
dataRDD.collect()
