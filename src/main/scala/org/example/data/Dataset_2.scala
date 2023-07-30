package org.example.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.wordCount.wordCount.sp

import scala.collection.mutable.ListBuffer

class Dataset_2{
  val sp = SparkSession.builder().appName("spark-ETL").master("local[*]").getOrCreate()
  sp.sparkContext.setLogLevel("ERROR")
  import sp.implicits._
  val rand = new scala.util.Random;
  val genreNames: Vector[String] = Vector("comedy", "horror", "mystery", "action", "animation", "fiction", "thriller", "fantasy", "adventure")

  def getDataset2(schema: Seq[String]): DataFrame= {

      val data = new ListBuffer[(Int, String)]

      for (i <- 1 to 100) {
        val genreId = i
        val genreName :String = getRanndomGenre()
        data += ((genreId, genreName))
      }
      val dataFrame :DataFrame = data.toDF(schema: _*)
      dataFrame
  }

  def getRanndomGenre(): String = {
        val generNamesLen = genreNames.length  //length of the genre vector
        val randomGenreId = rand.nextInt(generNamesLen)
        genreNames(randomGenreId)
  }
}
