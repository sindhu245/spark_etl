package org.example.testing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.example.testing.csv_read_check.spark

import scala.util.Try

class csv_read_check {

}

def check (): Try[DataFrame] = {
  import spark.implicits._

  var df = Seq((Some("a"), Some(1)), (null, null), (Some(""), Some(2))).toDF("A", "B")
  // df: org.apache.spark.sql.DataFrame = [A: string, B: int]
  //  df = df.select(coalesce(col("A"), lit(0)))
  println(df.show())


  print(df.select("A").count())
  Try {
    sparkSession.read
      .option("header", true)
      .option("basePath", basePath)
      .format(format)
      .load(allPaths: _*)
      .withColumnRenamed("Phone No.", "phone_no")
      .select("phone_no")
  }
}
object csv_read_check extends App{
  val spark = SparkSession.builder()
    .appName("blog.knoldus.com")
    .master("local")
    .getOrCreate()
//  val df = spark.read.option("header", true).format("csv").load("/Users/sindhu.patchipulusu/downloads/classification_result (2).csv")
//  val df_select = df.select("unique_id", "predicted_nccs")
//  val temp = df.drop("_c0")
//  temp.show(10)
}