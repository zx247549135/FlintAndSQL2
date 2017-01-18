package org.apache.spark.app.flint

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-24.
 */

class JoinChunk(size: Int = 10000000) extends ByteArrayOutputStream(size) { self =>



}

object FlintJoin {

  def main(args:Array[String]) {

    if (args.length < 4) {
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
    }).mapPartitions( iter => {
      val chunk = new JoinChunk()
      val dos = new DataOutputStream(chunk)
      for( (pageURL, pageRank, avgDuration) <- iter ){
        val length = pageURL.length
        dos.writeInt(length)
        dos.writeBytes(pageURL)
        dos.writeInt(pageRank)
        dos.writeInt(avgDuration)
      }
      Iterator(chunk)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val UVfile = sc.textFile(args(1)).map(s => {
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
    }).mapPartitions( iter => {
      val chunk = new JoinChunk()
      val dos = new DataOutputStream(chunk)

      for( (sourceIP,
      destURL,
      visitDate,
      adRevenue,
      userAgent,
      countryCode,
      languageCode,
      searchWord,
      duration) <- iter ){
        val length1 = sourceIP.length
        dos.writeInt(length1)
        dos.writeBytes(sourceIP)
        val length2 = destURL.length
        dos.writeInt(length2)
        dos.writeBytes(destURL)
        dos.writeFloat(adRevenue)
        val length3 = userAgent.length
        dos.writeInt(length3)
        dos.writeBytes(userAgent)
        dos.writeBytes(countryCode)
        dos.writeBytes(languageCode)
        val length4 = searchWord.length
        dos.writeInt(length4)
        dos.writeBytes(searchWord)
        dos.writeInt(duration)
      }
      Iterator(chunk)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //    // destURL, (sourceIp, adRevenue, visitDate)
    //    val UVfile_join = UVfile.map(line => (line._2, (line._1, line._4, line._3)))
    //      .filter(line => if (line._2._3 > date1 && line._2._3 < date2) true else false)
    //
    //    // pageURL, pageRank
    //    val Rfile_join = Rfile.map(line => (line._1, line._2))
    //
    //    // (URL, ((sourceIp, adRevenue, visitDate), pageRank))
    //    // => (sourceIp, (adRevenue, pageRank, 1))
    //    val joinFile = UVfile_join.join(Rfile_join).map(line => {
    //      val value = line._2
    //      (value._1._1, (value._1._2, value._2, 1))
    //    })

  }

}
