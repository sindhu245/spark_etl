package org.example.data

import org.apache.spark.sql.catalyst.expressions.CurrentRow.nullable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.testing.csv_read_check.spark
//import org.apache.spark.sql.functions.{broadcast, col, dense_rank, first, from_json, lit, lower, rank, row_number, udf, unbase64}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Temp extends App{

  def testing(sample: DataFrame) {
    sample.show()
//    println("------------------------->  "+ a, b)
//    val df: DataFrame = sample.filter(col("client")=== a && col("adv")=== b)
//    df.show()
    // Define a UDF to decode Base64-encoded string
    val decodeBase64UDF = udf((encoded: String) => new String(java.util.Base64.getDecoder.decode(encoded)))

    // Decode the Base64-encoded column
    val decodedDF = sample.withColumn("decoded_data", decodeBase64UDF(col("leadGen")))
    decodedDF.show(false)

    // Explode each key into separate columns
//    val explodedDF = decodedDF
//      .withColumn("json_struct", from_json(col("decoded_data"), MapType(StringType, StringType)))
//      .selectExpr("client", "adv", "explode(json_struct) as (key, value)")
//      .groupBy("client", "adv")
//      .pivot("key")
//      .agg(first("value"))
//
//    explodedDF.show(false)
//    println(a + "   " + b)
  }
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

//  val base64Df = Seq(
//    ("Audi", "adv1", "ewogICJnZW5kZXIiIDogIm1hbGUiLAogICJuYW1lIiA6ICJqYXNvbiBzY2VtYSIsCiAgInBob25lX251bWJlciIgOiA5ODc4OTM1NDc4LAogICJwaW5fY29kZSIgOiAxMTAwMDEsCiAgImRhdGUiIDogMTY0ODQ0NTU2NTM0LAogICJkZXNjcmlwdGlvbiIgOiAiTG9yZW0gaXBzdW0gbG9uZyB0ZXh0IiwKICAiY2l0eSIgOiAiTHVja25vdyIsCiAgInN0YXRlIiA6ICJVdHRhciBQcmFkZXNoIiwKICAidGVzdF9kcml2ZV9jYXIiIDogIkF1ZGkgNDQwcyIsCiAgInByZWZlcnJlZF9wYXltZW50X21ldGhvZHMiIDogWwogICAgInVwaSIsCiAgICAiY3JlZGl0X2NhcmQiCiAgXQp9")
////    ,
////    ("Audi", "adv2", "ewogICJnZW5kZXIiIDogIm1hbGUiLAogICJuYW1lIiA6ICJqYXNvbiBzY2VtYSIsCiAgInBob25lX251bWJlciIgOiA5ODc4OTM1NDc4LAogICJwaW5fY29kZSIgOiAxMTAwMDEsCiAgImRhdGUiIDogMTY0ODQ0NTU2NTM0LAogICJkZXNjcmlwdGlvbiIgOiAiTG9yZW0gaXBzdW0gbG9uZyB0ZXh0IiwKICAidGVzdF9kcml2ZV9jYXIiIDogIkF1ZGkgNDQwcyIsCiAgInByZWZlcnJlZF9wYXltZW50X21ldGhvZHMiIDogWwogICAgInVwaSIsCiAgICAiY3JlZGl0X2NhcmQiCiAgXQp9"),
////    ("Audi", "adv1", "ewogICJnZW5kZXIiIDogIm1hbGUiLAogICJuYW1lIiA6ICJqYXNvbiBzY2VtYSIsCiAgInBob25lX251bWJlciIgOiA5ODc4OTM1NDc4LAogICJwaW5fY29kZSIgOiAxMTAwMDEsCiAgImRhdGUiIDogMTY0ODQ0NTU2NTM0LAogICJkZXNjcmlwdGlvbiIgOiAiTG9yZW0gaXBzdW0gbG9uZyB0ZXh0IiwKICAiY2l0eSIgOiAiTHVja25vdyIsCiAgInN0YXRlIiA6ICJVdHRhciBQcmFkZXNoIiwKICAidGVzdF9kcml2ZV9jYXIiIDogIkF1ZGkgNDQwcyIsCiAgInByZWZlcnJlZF9wYXltZW50X21ldGhvZHMiIDogWwogICAgInVwaSIsCiAgICAiY3JlZGl0X2NhcmQiCiAgXQ")
//  ).toDF("client", "adv", "leadGen")
//
////  val temp = base64Df.withColumn("decode", decode(col("leadGen"), "UTF-8"))
////    .cast(MapType(StringType, StringType)
//  var temp = base64Df.withColumn("decode", unbase64(col("leadGen")).cast("string"))
//  temp.show(false)
////  val schema = schema_of_json(lit(temp.select($"decode").as[String].first))
//  temp = temp.withColumn("Notes",from_json(col("decode"),MapType(StringType,StringType)))
////  temp.select(col("client"), col("Notes")("gender")).show()
////  temp.show(false)
//  val allKeys = temp.select(explode(map_keys(col("Notes"))))
//  temp.selectExpr("Notes.*").show(false)
  // Dynamically create columns for each key
//  val resultDF = allKeys.foldLeft(df)((tempDF, key) => tempDF.withColumn(key, col("map_column")(key)))


  //  val schema = schema_of_json(lit(temp.select($"Notes").as[String].first))
//  val dfresult = temp.withColumn("jsonColumn", explode(from_json(col("decode"), MapType(StringType, StringType))))
//  dfresult.show(false)
//    .select($"id", $"name", $"jsonColumn.*")
//  dfresult.show(false)

//  val sample = base64Df.withColumn("rank_row", rank()
//    .over(Window.partitionBy("client","adv").orderBy(col("leadGen").desc)))
//  sample.show()
//  testing(sample.filter(col("rank_row")===1))

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

//  val df = spark.read.option("header", true).option("inferSchema", "true").csv("/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/internal/advertiser=Apple/ad_id=AD12345/cd=2024-02-26/").schema
////  df.show()
//  for (i <- df){
//    println(i)
//  }
//  val data = spark.read.schema(df).option("header", true).csv("/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/internal/advertiser=Apple/ad_id=AD12345/cd=2024-02-26/")
//  data.show()
//    .option("header", true).option("mergeSchema", true)


  // Define schema for first dataset
  val schema1 = StructType(Seq(
    StructField("col1", StringType, nullable = true),
    StructField("col2", StringType, nullable = true)
  ))

  // Define schema for second dataset
  val schema2 = StructType(Seq(
    StructField("col1", StringType, nullable = true),
    StructField("col3", StringType, nullable = true)
  ))

  // Merge schemas to create combined schema
  val combinedSchema = StructType(schema1.fields ++ schema2.fields.filterNot(_.name == "col1"))

  // Read data using combined schema
  val data = spark.read.schema(combinedSchema).option("header", true).option("sep", ",").csv("/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/sam/")

  // Show the data
  data.show()

}

