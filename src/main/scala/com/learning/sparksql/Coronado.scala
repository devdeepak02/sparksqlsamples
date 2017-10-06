package com.learning.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext



/**
  * Created by kumade1 on 10/6/17.
  */
object Coronado extends App{

  override def main(args: Array[String]): Unit = {
    val dbusername:String =""
    val dbpassword: String = ""
    val dbschema:String = ""

    val sparkConf  = new SparkConf().setAppName("Coronado")
    val sparkContext = new SparkContext("local[*]","",sparkConf)
    val sQLContext = new SQLContext(sparkContext)

    val airbnbDF = sQLContext.load("jdbc",
      Map("url" -> s"jdbc:postgresql://127.0.0.1/${dbschema}?username=${dbusername}&password=${dbpassword}",
        "dbtable" -> "airbnbNY"))
    airbnbDF.printSchema()
  }

}
