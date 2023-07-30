package org.example.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object wordCount{
  val rand = new scala.util.Random;
  val sp = SparkSession.builder().appName("spark-ETL").master("local[*]").getOrCreate()
  import sp.implicits._
  sp.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String])={

    var data:Vector[String] = Vector()
    for(i<- 1 to 10000){
        val attribute_len: Int = rand.nextInt(10) + 1
        val attribute: String = getRandomStrings(attribute_len)
        data = data :+ (attribute)
      }
    val result = getWordCount(data)
    val resultDataframe = result.toDF("String", "Count")
    resultDataframe.show()
    }

  def getRandomStrings(attribute_len: Int): String = {
    var attributes: String = ""
    for (i <- 1 to attribute_len) {
      val randomInt: Int = rand.nextInt(10) + 1
      attributes += (rand.alphanumeric.take(randomInt).mkString + ",")
    }
    attributes
  }

  private def getWordCount(data: Vector[String]): RDD[(String, Int)] = {
    val rdd = sp.sparkContext.parallelize(data,5)
    println(rdd.partitions.size)
    val flatten = rdd.flatMap(f => f.split(","))
    val convertToTuple = flatten.map(m => (m, 1))
    val reduceTheCountOfStrings = convertToTuple.reduceByKey(_ + _)
    val sortByInteger = reduceTheCountOfStrings.map(a => (a._2, a._1)).sortByKey()
    val stringIntFormat = sortByInteger.map(a => (a._2, a._1))
    stringIntFormat
  }
}
