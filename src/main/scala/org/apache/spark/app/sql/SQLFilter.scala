package org.apache.spark.app.sql

/**
 * Created by zx on 16-1-22.
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zx on 16-1-22.
 */
object SQLFilter {

  case class Rankings(pageURL: String, pageRank: Int, avgDuration: Int)

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val textRDD1 = sparkContext.textFile(args(0))
    val parameters = args(2)

    //    val schemaString1 = "pageURL pageRank avgDuration"
    //    val schema1 = StructType(schemaString1.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    //    val rankings = textRDD1.map(_.split(",")).map(line =>
    //      Row(line(0), line(1).toInt, line(2).toInt))
    //    val dataFrame_rankings = sqlContext.createDataFrame(rankings, schema1)
    //
    //    dataFrame_rankings.registerTempTable("rankings")
    //    sqlContext.cacheTable("rankings")
    //
    //    dataFrame_rankings.foreach(_ => Unit)

    import sqlContext.implicits._
    val rankings = textRDD1.map(_.split(",")).map(line => {
      Rankings(line(0), line(1).toInt, line(2).toInt)
    }).toDF()

    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings")

    val result =sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > " + parameters.toInt)

    //println("---count: " + result.count())

    result.toJavaRDD.saveAsTextFile(args(1))
  }

}
