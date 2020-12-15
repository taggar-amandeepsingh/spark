// Databricks notebook source
// /FileStore/tables/numberData.csv

def isPrime(i:Int) :Boolean = {
  if (i <=1)
    false
  else if (i==2)
    true
  else
    !(2 to (i-1)).exists( x=> i%x==0)
}

// COMMAND ----------

val numData= sc.textFile("/FileStore/tables/numberData.csv")
val header=numData.first()
val numData2=numData.filter(x=>x!=header)
val data=numData2.map(x=>x.toInt)

// COMMAND ----------

val resultData=data.map(x=> (x,isPrime(x)))
resultData.take(10)

// COMMAND ----------

resultData.filter( x => x._2 == true ).map( x => x._1).sum
