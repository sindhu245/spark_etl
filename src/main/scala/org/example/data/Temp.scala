package org.example.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, lit, lower}

import java.time.LocalDateTime

object Temp extends App{

//for(i <- 1 to 10) {
//  var current_time = System.currentTimeMillis()
//  //  println(new )
//
//  var temp: Long = 0
//  if (i >= 5) {
//    temp = current_time
//  }
//  else {
//    println()
//    temp = current_time + (2 * 365 * 24 * 60 * 60 * 1000)
//  }
//
//  var hello = current_time
//
//
//  def epochToDate(epochMillis: Long): String = {
//    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    df.format(epochMillis)
//  }
//
//  //  println("-----"+ epochToDate(hello)+ "------------"+epochToDate(temp)+"-----"+epochToDate(1753092440626L))
//  ////  +"\n")
//  //  println(63072000000L+1690020440626L)
//}

//  val cDateTime: LocalDateTime = LocalDateTime.now
//  println(cDateTime)
//  val nextyear = LocalDateTime.now.plusYears(2)
//  println(LocalDateTime.parse(nextyear.toString).isAfter(cDateTime))

//
//  val ctimestamp = Timestamp.valueOf(cDateTime)
//  val nextTime = Timestamp.valueOf(nextyear)
////  println(nextTime-ctimestamp)
//println(LocalDateTime.now)
//  println(Timestamp.valueOf(nextyear))




////  println(Long.valueOf(nextyear))
//  val obj = new loadData()
//    val data3 = obj.readDataframe("dataset_3")
//  println("\n"+data3.agg(sum("is_expired")).first.get(0))


//println("2025-07-22T17:43:16.328" > "2023-07-22T17:43:16.328")
//
//  val list: List[Int] = List(1,2,3,4,5)
//  println(list.find(_>3))

  val spark = SparkSession.builder()
    .appName("blog.knoldus.com")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  //  creating the employee directory which is bigger dataframe
  val employeeDF = Seq(
    ("Amit", "Bangalore"),
    ("Ankit", "California"),
    ("Abdul", "Pune"),
    ("Sumit", "California"),
    ("Riya", "Pune")
  ).toDF("first_name", "city")

  //  creating the citiesDf which is small df that will be broadcasted
  var citiesDF = Seq(
    ("15K-20K", "Usa"),
    ("Bangalore", "india"),
    ("Pune", "India")
  ).toDF("city", "country")

  //  Now we will perform the join operation on employeeDF with broadcasted citiesDF

//  var joinedDf = employeeDF.join(broadcast(citiesDF), employeeDF.col("city") === citiesDF.col("city"))
//
//  //  Now we will drop the city column from citiesDF as we don't want to keep duplicate column
//  joinedDf = joinedDf.drop(citiesDF.col("city"))
//
//  //  Finally we will see the joinedDF
//  joinedDf.show()
//
//  private var joinedDf1 = employeeDF.join(citiesDF, employeeDF.col("city") === citiesDF.col("city"))
//  joinedDf1 = joinedDf1.drop(citiesDF.col("city"))
//  joinedDf1.show()

//  var df = spark.read.parquet("s3a://hotstar-ads-targeting-us-east-1-prod/trackers/advertiser_reporting/qc/hourly/cd=2023-10-04/hr=13/")

//  df = df.withColumn("blaze_impression_percentage_match", ((col("advertiser_blaze_impression")).divide(col("data_lake_blaze_impression"))))

//  df.show()
  citiesDF = citiesDF.withColumn("temp", lower(col("city")))
//  temp = lower(temp)
  print(citiesDF.head)
}

