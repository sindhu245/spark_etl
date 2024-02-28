package org.example.testing

import com.oracle.webservices.internal.api.databinding.DatabindingModeFeature

import java.util.zip.{ZipEntry, ZipOutputStream}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection.Schema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat, date_format, explode, from_json, from_unixtime, from_utc_timestamp, lit, map_keys, to_json, udf, unbase64}
import org.apache.spark.sql.types.{DataTypes, MapType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

class AdGenericLead(spark:SparkSession){
  val cteJson = "/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/cte.json/"
  def load(): DataFrame={
    spark.read.json(cteJson)
  }

  def transform(dataFrame: DataFrame, cd: String, hr: String): DataFrame = {
    val df = dataFrame.withColumn("distinct_adId", concat(col("advertiserName"), concat(lit("_"), col("adId"))))
      .withColumn("created_on", getEpochInSeconds(col("timestamp")).cast(DataTypes.LongType))
      .withColumn("created_on", from_utc_timestamp(from_unixtime(col("created_on")), "Asia/Kolkata"))
      .withColumn("created_on", date_format(col("created_on"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("cd", lit(cd))
      .withColumn("hour", lit(hr))

    val distinctAdId = df.select("distinct_adId").distinct().collect()

    df.persist(StorageLevel.MEMORY_AND_DISK)
    if (!distinctAdId.isEmpty) {
      distinctAdId.foreach(row => {
        val advertiser_list = row.mkString.split("_")
        val advertiserName = advertiser_list(0)
        val adId = advertiser_list(1)
        val leadGenDf = df.filter(col("advertiserName") === advertiser_list(0) &&
          col("adId") === advertiser_list(1))
        var decodedDf = decodeBase64column(leadGenDf)
        decodedDf = decodedDf.drop("leadGenData", "distinct_adId", "decoded_data", "clientAdId", "dwId", "timestamp")
        decodedDf.show()

        //internal
        val internalPath = "/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/internal/" + "advertiser=" + advertiserName + "/ad_id=" + adId
        write(decodedDf, internalPath, "internal", cd, hr)

        val outputPath = "/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/output/" + "advertiser=" + advertiserName + "/ad_id=" + adId
        val data = getDatafromInternalBucket(advertiser_list(0), advertiser_list(1), cd)
        data.show()
        write(data, outputPath, "external", cd, hr)
      })
    }

    df
  }

  def write(dataFrame: DataFrame, outputPath: String, location: String,cd: String, hr: String): Unit = {
    val format = "csv"
    val compression = "gzip"
    val reportName = "Report_" + cd
    var path_1 = outputPath
    var partitions = List("")
    if (location == "internal") {
      path_1 = path_1 + "/cd=" + cd + "/hour=" + hr
      partitions = List("cd", "hour")
    }
    else if (location == "external") {
      path_1 = path_1 + "/cd=" + cd
      partitions = List("cd")
    }

    dataFrame.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format(format)
      .partitionBy(partitions: _*)
      .option("compression", compression)
      .option("header", true)
      .option("quoteAll", true)
      .save(outputPath)

    println(path_1)
    val src = new Path(path_1)
    val fs = src.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val fileName = fs.globStatus(new Path(path_1 + "/part-*"))(0).getPath().getName()
    fs.rename(new Path(path_1 + "/" + fileName), new Path(path_1 + "/" + reportName + ".csv.gz"))

    //    ZipUtil.pack(new File("/tmp/demo"), new File("/tmp/demo.zip"))

    //TODO compression in zip format
    //ToDo columns to extract?
  }

  def getDatafromInternalBucket(advertiserName: String, adId: String, cd: String): DataFrame={
//    val partitionPath = "advertiser=" + advertiserName + "/ad_id=" + adId + "/cd="+ System.getProperty("cd")
    val internalPath = "/Users/sindhu.patchipulusu/IdeaProjects/spark-etl/src/main/scala/org/example/testing/internal/" + "advertiser=" + advertiserName + "/ad_id=" + adId
//    val schema: Schema = Schema("\"adId\",\"advertiserName\",\"loginStatus\",\"created_on\",\"foods_intersted\",\"phone_number\",\"email\",\"city\",\"state\",\"hr\"")
    spark.read.option("header", true).csv(internalPath)
  }

  def decodeBase64column(dataFrame: DataFrame): DataFrame = {
    //decoded column to convert to column
    val decodeBase64Df = dataFrame.withColumn("decoded_data", unbase64(col("leadGenData")).cast("string"))

    //Convert the json column to map<string, string>
    var decodeDf = decodeBase64Df.withColumn("decoded_data", from_json(col("decoded_data"), MapType(StringType, StringType)))
    //TODO - think a better way to get the schema rather than explode
    var allKeys = decodeDf.select(explode(map_keys(col("decoded_data"))))
    allKeys = allKeys.distinct()
    allKeys.collect().foreach(row =>
      decodeDf = decodeDf.withColumn(row.mkString, col("decoded_data")(row.mkString))
    )
    val json_schema = allKeys.rdd.map(r => r.getString(0)).collect().toList
    val schema = json_schema ++ List("loginStatus","created_on", "cd", "hour")
    decodeDf.selectExpr(schema: _*)
  }

  def getEpochInSeconds: UserDefinedFunction = udf[Long, String](client_time => {
    var rv: Long = 0
    try {
      if (client_time != null && client_time.length > 10) {
        rv = client_time.substring(0, 10).toLong
      }
    } catch {
      case _: Exception =>
    }

    rv
  })
}

object AdGenericLead extends App{
  val spark = SparkSession.builder()
    .appName("blog.knoldus.com")
    .master("local")
    .getOrCreate()
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  val ad_obj = new AdGenericLead(spark)
  val loadDF = ad_obj.load()
  ad_obj.transform(loadDF, "2024-02-26", "03")
}
