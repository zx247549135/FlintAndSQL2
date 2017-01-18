package org.apache.spark.app.sql

import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-22.
 */
object SQLWC {

  case class UserVisits(sourceIP: String,
                        destURL: String,
                        visitDate: Long,
                        adRevenue: Float,
                        userAgent: String,
                        countryCode: String,
                        languageCode: String,
                        searchWord: String,
                        duration: Int)

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val textRDD1 = sparkContext.textFile(args(0))
    val parameters = args(2).toInt

    //    val schemaString1 = "pageURL pageRank avgDuration"
    //    val schema1 = StructType(schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //    val rankings = textRDD1.map(_.split(",")).map(line =>
    //      Row(line(0), line(1).toInt, line(2).toInt))
    //    val dataFrame_rankings = sqlContext.createDataFrame(rankings, schema1)
    //
    //    dataFrame_rankings.registerTempTable("rankings")
    //    sqlContext.cacheTable("rankings")
    //
    //    dataFrame_rankings.foreach(_ => Unit)

    import sqlContext.implicits._
    val userVisits = textRDD1.map(_.split(",")).map(line => {
      val dateInput = line(2).split("-").map(_.toInt)
      val date = new Date(dateInput(0), dateInput(1), dateInput(2))
      UserVisits(line(0), line(1), date.getTime , line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }).toDF()

    userVisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits")

    val result =sqlContext.sql("SELECT SUBSTR(sourceIP, 1, " + parameters + "), SUM(adRevenue) FROM uservisits " +
      " GROUP BY SUBSTR(sourceIP, 1, " + parameters + ")")

    //println("---count: " + result.count())

    result.toJavaRDD.saveAsTextFile(args(1))
  }

}
