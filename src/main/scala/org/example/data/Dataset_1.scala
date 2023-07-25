package org.example.data

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.util.UUID
import scala.collection.mutable.ListBuffer

class Dataset_1{
  val rand = new scala.util.Random;
  val sp = SparkSession.builder().appName("spark-ETL").master("local[*]").getOrCreate()
  import sp.implicits._

  def getDataset1(schema: Seq[String]): DataFrame= {
      val data = new ListBuffer[(String, Integer, Float, String, Long, String, Int)]

      for (i <- 1 to 10000) {
        val current_time =  LocalDateTime.now

        val uuid = UUID.randomUUID().toString
        val score: Int = rand.nextInt(1000) + 1
        val price = rand.nextFloat() * 100 + 1
        val attribute_len: Int = rand.nextInt(10) + 1
        val attribute: String = getRandomStrings(attribute_len)
        val timeStamp = current_time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
        //      val expiry: TimeStamp = if (i>=5000) new TimeStamp(System.currentTimeMillis()) else new TimeStamp(System.currentTimeMillis())
        val expiry = if (i >= 5000) current_time.toString else current_time.plusYears(2).toString
        val genre: Int = rand.nextInt(100) + 1

        data += ((uuid, score, price, attribute, timeStamp, expiry, genre))
      }

      val dataFrame: DataFrame = data.toDF(schema: _*)
      dataFrame
  }

  def getRandomStrings(attribute_len: Int): String = {
    var attributes: String = ""
    for (i <- 1 to attribute_len){
      val randomInt: Int = rand.nextInt(10)+1
//      attributes = attributes :+ rand.alphanumeric.take(randomInt).mkString
      attributes += (rand.alphanumeric.take(randomInt).mkString + ",")
    }
    attributes
  }
}
