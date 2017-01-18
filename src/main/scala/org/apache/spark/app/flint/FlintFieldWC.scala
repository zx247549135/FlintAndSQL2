package org.apache.spark.app.flint

import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel

/**
 * Created by zx on 16-11-14.
 */

object FlintFieldWC {

  def main(args: Array[String]): Unit = {

    if(args.length < 4){
      System.err.println("Usage of Parameters: inputPath,outputPath,X,appName,storageLevel")
      System.exit(1)
    }

    val storageLevel = args(4).toInt match {
      case 1 => StorageLevel.MEMORY_AND_DISK
      case 2 => StorageLevel.MEMORY_AND_DISK_SER
      case _ => StorageLevel.MEMORY_AND_DISK
    }

    val compress = args(5).toInt match {
      case 0 => false
      case 1 => true
      case _ => false
    }

    val X = args(2).toInt
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val file = sparkContext.textFile(args(0)).map( _.split(',')).mapPartitions({ iter =>
      val chunk = new FieldChunk(X)
      chunk.registerChunk("sourceIP")
      chunk.registerChunk("destURL")
      chunk.registerChunk("Date")
      chunk.registerChunk("adRevenue")
      chunk.registerChunk("userAgent")
      chunk.registerChunk("countryCode")
      chunk.registerChunk("languageCode")
      chunk.registerChunk("searchWord")
      chunk.registerChunk("duration")
      var count = 0

      for(list <- iter){
        count += 1

        // sourceIP: String, destURL: String
        chunk.putUTF("sourceIP", list(0))
        chunk.putUTF("destURL", list(1))

        // Date: Long
        val dateString = list(2).split('-').map(_.toInt)
        chunk.putLong("Date", new Date(dateString(0),dateString(1),dateString(2)).getTime)

        // adRevenue: Float
        chunk.putDouble("adRevenue",list(3).toDouble)

        // userAgent, countryCode, languageCode, searchWord : String
        // userAgent and searchWord are var, but countryCode and languageCode are val 3 or 6
        chunk.putUTF("userAgent", list(4))
        chunk.putUTF("countryCode", list(5))
        chunk.putUTF("languageCode", list(6))
        chunk.putUTF("searchWord", list(7))

        // duration: Int
        chunk.putInt("duration", list(8).toInt)
      }
      chunk.setRecords(count)
      if(compress)
        chunk.compressBytes()
      Iterator(chunk)
    }).persist(storageLevel)

    //file.foreach(_ => Unit)

    val result = file.mapPartitions{ iter =>
      val chunk = iter.next()
      val result = chunk.getVectorValueIterator()
      result
    }.map(t => (t._1.hashCode(), t._2)).reduceByKey( _ + _ )
    result.saveAsTextFile(args(1))
    //result.foreach(println)
    sparkContext.stop()

  }

}
