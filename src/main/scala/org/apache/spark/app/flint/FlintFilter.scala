package org.apache.spark.app.flint

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-22.
 */

class WordChunk extends ByteArrayOutputStream{ self =>


  def getFilter(regex:Int)= new Iterator[(String,Int)]{
    var offset = 0
    var currentKey = ""
    var currentValue = 0

    override def hasNext= {
      if (offset < self.count) {
        var matched = false
        var end = false
        while (!matched && !end) {
          val urlLength = WritableComparator.readInt(buf, offset)
          offset += (urlLength + 4)
          currentValue = WritableComparator.readInt(buf, offset)
          if (currentValue > regex) {
            currentKey = new String(buf, offset - urlLength, urlLength)
            matched = true
          }
          offset += 8
          if(offset >= self.count){
            end = true
          }
        }
        matched
      }
      else
        false
    }

    override def next()= {

      if (currentKey == "" && currentValue == 0) Iterator.empty.next()
      else
        (currentKey,currentValue)
    }
  }
}

object FlintFilter {

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))

    val words = lines.map{ s=>
      val parts = s.split(",")
      (parts(0),parts(1).toInt, parts(2).toInt)
    }

    val chunkWords = words.mapPartitions ( iter => {
      val chunk = new WordChunk
      val dos = new DataOutputStream(chunk)
      for ((url, rank, avgDuration) <- iter) {
        dos.writeInt(url.length)
        dos.writeBytes(url)
        dos.writeInt(rank)
        dos.writeInt(avgDuration)
      }
      Iterator(chunk)
    }).persist(StorageLevel.MEMORY_AND_DISK)


    val finalWords = chunkWords.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getFilter(args(2).toInt)
    })

    finalWords.saveAsTextFile(args(1))
  }

}
