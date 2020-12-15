// Databricks notebook source
// /FileStore/tables/FriendsData.csv

val friendsData=sc.textFile("/FileStore/tables/FriendsData.csv")
val removeHeader = friendsData.filter( x=> !x.contains("name"))

// COMMAND ----------

val ageFriends= removeHeader.map( x=> (x.split(",")(2).toInt, (1, x.split(",")(3).toInt) ))

// COMMAND ----------

val reducedRDD=ageFriends.reduceByKey( (x,y) => (x._1+y._1 , x._2+y._2) )

// COMMAND ----------

val averageAge=reducedRDD.mapValues( x => x._2/x._1)

// COMMAND ----------

val ageRDD= removeHeader.map( x=> (x.split(",")(2), x.split(",")(3).toInt ))
val maxKey2 = ageRDD.max()(new Ordering[Tuple2[String, Int]]() {
  override def compare(x: (String, Int), y: (String, Int)): Int = 
      Ordering[Int].compare(x._2, y._2)
})
