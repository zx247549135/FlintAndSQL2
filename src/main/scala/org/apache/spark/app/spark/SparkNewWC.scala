package org.apache.spark.app.spark

import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zx on 16-1-22.
 */
object SparkNewWC {

  def main(args: Array[String]){
    if(args.length<4){
      System.err.println("Usage of Parameters: inputPath,X,outputPath,appName,storageLevel")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val subIndex = args(2).toInt
    val storageLevel = args(4).toInt match {
      case 1 => StorageLevel.MEMORY_AND_DISK
      case 2 => StorageLevel.MEMORY_AND_DISK_SER
      case _ => StorageLevel.MEMORY_AND_DISK
    }

    val finalWords = lines.map(s => {
      val parts = s.split(",")
      val dateInput = parts(2).split("-").map(_.toInt)
      val date = new Date(dateInput(0), dateInput(1), dateInput(2))
      (parts(0),
        parts(1),
        date.getTime,
        parts(3).toDouble,
        parts(4),
        parts(5),
        parts(6),
        parts(7),
        parts(8).toInt)
    }).persist(storageLevel)

    val result = finalWords.map(line => (line._1.substring(0,subIndex).hashCode(), line._4)).reduceByKey(_ + _)

    result.saveAsTextFile(args(1))
  }

}
