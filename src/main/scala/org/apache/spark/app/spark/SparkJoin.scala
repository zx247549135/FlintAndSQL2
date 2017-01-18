package org.apache.spark.app.spark

import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-22.
 */
object SparkJoin {


  def main(args:Array[String]){
    if(args.length<4){
      System.err.println("The Usage Of Parameters: inputRanking, inputUserVisits, data(format as  year-month-day),appName")
      System.exit(1)
    }
    val date1 = new Date(1980, 1, 1).getTime
    val date2Input = args(2).split("-").map(_.toInt)
    val date2 = new Date(date2Input(0), date2Input(1), date2Input(2)).getTime

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)

    val Rfile = sc.textFile(args(0)).map(s => {
      val parts = s.split(",")
      (parts(0), parts(1).toInt, parts(2).toInt)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val UVfile=sc.textFile(args(1)).map(s => {
      val parts = s.split(",")
      val dateInput = parts(2).split("-").map(_.toInt)
      val date = new Date(dateInput(0), dateInput(1), dateInput(2))
      (parts(0),
        parts(1),
        date.getTime,
        parts(3).toFloat,
        parts(4),
        parts(5),
        parts(6),
        parts(7),
        parts(8).toInt)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    // destURL, (sourceIp, adRevenue, visitDate)
    val UVfile_join = UVfile.map(line => (line._2, (line._1, line._4, line._3)))
      .filter(line => if(line._2._3 > date1 && line._2._3 < date2) true else false)

    // pageURL, pageRank
    val Rfile_join = Rfile.map(line => (line._1, line._2))

    // (URL, ((sourceIp, adRevenue, visitDate), pageRank))
    // => (sourceIp, (adRevenue, pageRank, 1))
    val joinFile = UVfile_join.join(Rfile_join).map(line => {
      val value = line._2
      (value._1._1, (value._1._2, value._2, 1))
    })

    // sourceIp, (SUM(adRevenue), SUM(pageRank), SUM)
    // => AVG(pageRank), (sourceIp, SUM(adRevenue))
    val results = joinFile.reduceByKey( (value1, value2) =>
      (value1._1 + value2._1, value1._2 + value2._2, value1._3 + value2._3)).map(line =>
      (line._2._2 / line._2._3, (line._1, line._2._1))).sortByKey(false)

    val result = results.first()
    println("result: " + result._2._1 + ", " + result._1 + ", " + result._2._2)
    results.saveAsTextFile(args(4))

  }


}
