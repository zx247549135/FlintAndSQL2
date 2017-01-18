package org.apache.spark.app.flint

import java.io.DataOutputStream

import org.apache.hadoop.io.WritableComparator

import scala.collection.mutable

/**
 * Created by zx on 16-11-14.
 */

class FieldChunkPage(size: Int = 10485760) extends SlowGrowCompressByteStream {

  var currentPosition = 0

  def readUTF(): String = {
    var utfLength = WritableComparator.readUnsignedShort(buf, currentPosition)
    currentPosition += 2
    val result = new String(buf, currentPosition, utfLength, "utf-8")
    currentPosition += utfLength
    result
  }

  def readDouble(): Double = {
    val result = WritableComparator.readDouble(buf, currentPosition)
    currentPosition += 8
    result
  }

}

class FieldChunk(X: Int) extends Serializable{

  // Records
  var records = 0

  def setRecords(recordsCount: Int): Unit ={
    records = recordsCount
  }

  // Fields
  val chunkMaps = new mutable.HashMap[String, FieldChunkPage]()

  def registerChunk(name: String): Unit ={
    chunkMaps.put(name, new FieldChunkPage())
  }

  def putUTF(name: String, value: String): Unit ={
    val dos = new DataOutputStream(chunkMaps.get(name).get)
    dos.writeUTF(value)
  }

  def putInt(name: String, value: Int): Unit ={
    val dos = new DataOutputStream(chunkMaps.get(name).get)
    dos.writeInt(value)
  }

  def putDouble(name: String, value: Double): Unit ={
    val dos = new DataOutputStream(chunkMaps.get(name).get)
    dos.writeDouble(value)
  }

  def putLong(name: String, value: Long): Unit ={
    val dos = new DataOutputStream(chunkMaps.get(name).get)
    dos.writeLong(value)
  }

  // Compress
  var compress: Boolean = false

  def compressBytes(): Unit = {
    compress = true
    for((name, uncompressed) <- chunkMaps){
      uncompressed.compress()
    }
  }

  def decompressBytes(name: String): Array[Byte] ={
    chunkMaps.get(name).get.decompress()
  }

  // process
  def getVectorValueIterator(): Iterator[(String, Double)] = {

    if(compress) {
      val decompressedBytes1 = decompressBytes("sourceIP")
      val decompressedBytes2 = decompressBytes("adRevenue")

      return new Iterator[(String, Double)]{
        var readRecords = 0
        var offset1 = 0
        var offset2 = 0

        def hasNext(): Boolean = readRecords < records

        def next(): (String, Double) = {
          if (!hasNext) Iterator.empty.next()
          readRecords += 1
          val utfLength = WritableComparator.readUnsignedShort(decompressedBytes1, offset1)
          offset1 += 2
          val sourceIpString = new String(decompressedBytes1, offset1, utfLength, "utf-8")
          offset1 += utfLength
          var XTemp = X
          if (X > sourceIpString.length)
            XTemp = sourceIpString.length
          val sourceIp = sourceIpString.substring(0, XTemp)
          val adRevenue = WritableComparator.readDouble(decompressedBytes2, offset2)
          offset2 += 8
          (sourceIp, adRevenue)
        }
      }
    }

    return new Iterator[(String, Double)] {

      var readRecords = 0
      val chunk1 = chunkMaps.get("sourceIP").get
      val chunk2 = chunkMaps.get("adRevenue").get

      def hasNext(): Boolean = readRecords < records

      def next(): (String, Double) = {
        if (!hasNext) Iterator.empty.next()
        readRecords += 1
        val sourceIpString = chunk1.readUTF()
        var XTemp = X
        if (X > sourceIpString.length)
          XTemp = sourceIpString.length
        val sourceIp = sourceIpString.substring(0, XTemp)
        val adRevenue = chunk2.readDouble()
        (sourceIp, adRevenue)
      }

    }
  }

}
