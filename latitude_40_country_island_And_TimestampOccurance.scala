// Databricks notebook source
// /FileStore/tables/airports.text

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/816254878439192/1690453240827080/6894355921072367/latest.html

val airportData= sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

// DBTITLE 1,Latitude>40 or Country=Island
val dataFilter = airportData.filter( x=> {
  (x.split(",")(6) > "\"40\"") || (x.split(",")(3).contains("Island"))
})
dataFilter.take(4)

// COMMAND ----------

dataFilter.saveAsTextFile("Island.csv")

// COMMAND ----------

// DBTITLE 1,Timestamp Occurance
val TimeOccurance=airportData.filter(x=>{
  (x.split(",")(11).contains("Pacific/Port_Moresby")) && ((x.split(",")(8).toInt)%2==0)
})
TimeOccurance.count()
