package org.apache.spark.app.flint

import java.io.DataOutputStream
import java.util.Date

import org.apache.hadoop.io.WritableComparator
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.xerial.snappy.Snappy

/**
 * Created by zx on 16-11-14.
 */

class AggreChunk3(X:Int,size:Int=33554432) extends SlowGrowByteStream(size){ self =>

  var compress: Boolean = false

  def compressBytes(): Unit = {
    compress = true
    val tmpBuf : Array[Byte] = Snappy.compress(buf)
    buf = tmpBuf
  }

  def decompressBytes(): Array[Byte] ={
    Snappy.uncompress(buf)
  }

  def getVectorValueIterator(): Iterator[(String, Double)] = {

    val chunk = if(compress) decompressBytes() else buf

    new Iterator[(String, Double)] {

      var offset = 0

      override def hasNext = offset < self.count

      override def next() = {
        if (!hasNext) Iterator.empty.next()
        else {
          // sourceIp
          var utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2
          val sourceIp = new String(chunk, offset, utfLength, "utf-8")
          offset += utfLength
          var XTemp = X
          if (X > sourceIp.length)
            XTemp = sourceIp.length
          val SourceIp = sourceIp.substring(0, XTemp)

          // destURL
          utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2 + utfLength
          // date
          offset += 8
          // adRevenue
          val adRevenue = WritableComparator.readDouble(chunk, offset)
          offset += 8
          // userAgent, countryCode, languageCode, searchWord : String
          utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2 + utfLength
          utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2 + utfLength
          utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2 + utfLength
          utfLength = WritableComparator.readUnsignedShort(chunk, offset)
          offset += 2 + utfLength
          // duration
          offset += 4
          (SourceIp, adRevenue)
        }
      }
    }
  }
}

object FlintNewWC3 {

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
      val chunk = new AggreChunk3(X)
      val dos = new DataOutputStream(chunk)
      for( list <- iter){

        // sourceIP: String, destURL: String
        dos.writeUTF(list(0))
        dos.writeUTF(list(1))

        // Date: Long
        val dateString = list(2).split('-').map(_.toInt)
        dos.writeLong(new Date(dateString(0),dateString(1),dateString(2)).getTime)

        // adRevenue: Float
        dos.writeDouble(list(3).toDouble)

        // userAgent, countryCode, languageCode, searchWord : String
        // userAgent and searchWord are var, but countryCode and languageCode are val 3 or 6
        dos.writeUTF(list(4))
        dos.writeUTF(list(5))
        dos.writeUTF(list(6))
        dos.writeUTF(list(7))

        // duration: Int
        dos.writeInt(list(8).toInt)
      }
      if(compress)
        chunk.compressBytes()
      Iterator(chunk)
    }).persist(storageLevel)

    // file.foreach(_ => Unit)

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
