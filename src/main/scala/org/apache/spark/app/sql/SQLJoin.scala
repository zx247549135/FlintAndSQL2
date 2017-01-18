package org.apache.spark.app.sql

import java.util.Date

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-22.
 */
object SQLJoin {

  case class Rankings(pageURL: String, pageRank: Int, avgDuration: Int)

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
    val sparkConf = new SparkConf().setAppName("SQLJoin")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val textRDD1 = sparkContext.textFile(args(0))
    val textRDD2 = sparkContext.textFile(args(1))
    val parameters = args(2).split("-").map(_.toInt)
    val date2 = new Date(parameters(0), parameters(1), parameters(2)).getTime

    //    val schemaString1 = "pageURL pageRank avgDuration"
    //    val schema1 = StructType(schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //    val rankings = textRDD1.map(_.split(",")).map(line =>
    //      Row(line(0), line(1).toInt, line(2).toInt))
    //    val dataFrame_rankings = sqlContext.createDataFrame(rankings, schema1)
    //
    //    val schemaString2 = "sourceIp visitDate adRevenue userAgent countryCode languageCode searchWord duration"
    //    val schema2 = StructType(schemaString2.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //    val userVisits = textRDD2.map(_.split(",")).map(line => {
    //      val visitData = new Date(line(2))
    //      Row(line(0), line(1), visitData,
    //        line(3).toFloat, line(4), line(5),
    //        line(6), line(7), line(8).toInt)
    //    })
    //    val dataFrame_userVisits = sqlContext.createDataFrame(userVisits, schema2)
    //
    //    dataFrame_rankings.registerTempTable("rankings")
    //    dataFrame_userVisits.registerTempTable("userVisits")
    //    sqlContext.cacheTable("rankings")
    //    sqlContext.cacheTable("userVisits")

    val rankings = textRDD1.map(_.split(",")).map(line => {
      Rankings(line(0), line(1).toInt, line(2).toInt)
    }).toDF()

    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings")

    val userVisits = textRDD2.map(_.split(",")).map(line => {
      val dateString = line(2).split("-").map(_.toInt)
      val date = new Date(dateString(0), dateString(1), dateString(2))
      UserVisits(line(0), line(1), date.getTime , line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }).toDF()

    userVisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits")

    val date1 = new Date(1980, 1, 1).getTime

    val result1 = sqlContext.sql(" SELECT sourceIP, AVG(pageRank) as " +
      "avgPageRank, SUM(adRevenue) as totalRevenue FROM rankings AS R, uservisits as UV " +
      "WHERE R.pageURL = UV.destURL AND UV.visitDate > " + date1 +" AND UV.visitDate < " + date2 +
      " GROUP BY UV.sourceIP ")

    result1.registerTempTable("temp")

    val result = sqlContext.sql("SELECT sourceIP, totalRevenue, avgPageRank FROM temp ORDER BY totalRevenue DESC LIMIT 1")

    result.toJavaRDD.saveAsTextFile(args(3))

  }

}

