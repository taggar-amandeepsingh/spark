// Databricks notebook source
// /FileStore/tables/nasa_august.tsv
// /FileStore/tables/nasa_july.tsv

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/816254878439192/2510425955401590/6894355921072367/latest.html

val julydata = sc.textFile("/FileStore/tables/nasa_august.tsv")
val augustdata = sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val julyHost=julydata.map( x => x.split("\t")(0))
val augustHost=augustdata.map( x => x.split("\t")(0))

// COMMAND ----------

var intersectRDD = julyHost.intersection(augustHost)

// COMMAND ----------

def headerRemover(line: String): Boolean= !(line.startsWith("host"))
intersectRDD.filter(x=>headerRemover(x)).count()
//intersectRDD.count()
