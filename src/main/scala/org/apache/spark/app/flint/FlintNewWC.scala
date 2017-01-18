package org.apache.spark.app.flint

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util
import java.util.Date

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-23.
 */

class AggreChunk(X:Int ,size: Int = 128) extends  SlowGrowByteStream(size: Int){ self =>

  def getVectorValueIterator() = new Iterator[(String, Double)]{
    var offset=0
    override def hasNext=offset<self.count

    override def next()={
      if (!hasNext) Iterator.empty.next()
      else {
        val length = WritableComparator.readInt(buf,offset)
        offset += 4
        var Xtemp = X
        if( X > length )
          Xtemp = length
        val SourceIp = new String(buf, offset, Xtemp)
        offset += length
        val length2 = WritableComparator.readInt(buf, offset)
        offset = offset + 4 + length2

        offset += 8
        val adRevenue = WritableComparator.readDouble(buf, offset)
        offset += 8
        var lengthTemp1 = WritableComparator.readInt(buf, offset)
        offset = offset + 4 + lengthTemp1
        lengthTemp1 = WritableComparator.readInt(buf, offset)
        offset = offset + 4 + lengthTemp1
        lengthTemp1 = WritableComparator.readInt(buf, offset)
        offset = offset + 4 + lengthTemp1
        lengthTemp1 = WritableComparator.readInt(buf, offset)
        offset = offset + 4 + lengthTemp1

        offset += 4
        (SourceIp, adRevenue)
      }
    }
  }
}

object FlintNewWC {


  def main(args: Array[String]): Unit = {

    if(args.length < 4){
      System.err.println("Usage of Parameters: inputPath,outputPath,X,appName")
      System.exit(1)
    }

    val X = args(2).toInt
    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val file = sparkContext.textFile(args(0)).map( _.split(',')).mapPartitions({ iter =>
      val chunk = new AggreChunk(X)
      val dos = new DataOutputStream(chunk)
      var tmpSize = 0
      for( list <- iter){

        // sourceIP: String, destURL: String
        val length = list(0).length
        dos.writeInt(length)
        dos.writeBytes(list(0))

        val length1 = list(1).length
        dos.writeInt(length1)
        dos.writeBytes(list(1))

        // Date: Long
        val dateString = list(2).split('-').map(_.toInt)
        dos.writeLong(new Date(dateString(0),dateString(1),dateString(2)).getTime)

        // adRevenue: Float
        dos.writeDouble(list(3).toDouble)

        // userAgent, countryCode, languageCode, searchWord : String
        // userAgent and searchWord are var, but countryCode and languageCode are val 3 or 6
        val length4 = list(4).length
        dos.writeInt(length4)
        dos.writeBytes(list(4))
        dos.writeInt(list(5).length)
        dos.writeBytes(list(5))
        dos.writeInt(list(6).length)
        dos.writeBytes(list(6))
        val length7 = list(7).length
        dos.writeInt(length7)
        dos.writeBytes(list(7))

        // duration: Int
        dos.writeInt(list(8).toInt)
        println("Length: " + (chunk.size()-tmpSize))
        tmpSize = chunk.size()
      }
      Iterator(chunk)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    file.foreach(_ => Unit)

    val result = file.mapPartitions{ iter =>
      val chunk = iter.next()
      val result = chunk.getVectorValueIterator()
      result
    }.map(t => (t._1.hashCode(), t._2)).reduceByKey( _ + _ )
    //result.saveAsTextFile(args(1))
    result.foreach(println)
    sparkContext.stop()

  }
}
