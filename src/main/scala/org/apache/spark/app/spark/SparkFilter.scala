package org.apache.spark.app.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zx on 16-1-22.
 */
object SparkFilter {

  def main(args: Array[String]){
    if(args.length<4){
      System.err.println("Usage of Parameters: inputPath outputPath X appName")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    //val sparkContext = FlintWCContext.create(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val threshold = args(2).toInt
    val finalWords = lines.map(s => {
      val parts = s.split(",")
      (parts(0), parts(1).toInt, parts(2).toInt)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    finalWords.foreach(_ => Unit)

    val result = finalWords.filter(_._2 > threshold).map(s => (s._1, s._2))
    result.saveAsTextFile(args(1))
  }

}
