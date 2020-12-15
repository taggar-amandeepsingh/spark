// Databricks notebook source
// /FileStore/tables/airports.text

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/816254878439192/1166594940938316/6894355921072367/latest.html

val airportData = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val dataRDD= airportData.map (x=> ( x.split(",")(3), x.split(",")(11).toLowerCase() ))
dataRDD.collect()
