//package com.hotstar.ads_data.driver.nccs
//
//import com.hotstar.ads_data.config.ApplicationProperties
//import com.hotstar.ads_data.driver.nccs.common.{NCCSCommonUtils, UserDefinedFunctions}
//import com.hotstar.ads_data.utils.SparkUtil
//import com.typesafe.config.Config
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.storage.StorageLevel
//import org.joda.time.format.DateTimeFormat
//import org.slf4j.LoggerFactory
//
//import java.net.URI
//import scala.collection.JavaConverters.asScalaBufferConverter
//import scala.collection.mutable.ListBuffer
//import scala.util.{Failure, Success, Try}
//
//
//class KantarTransformDataWorkflow {
//
//  val logger = LoggerFactory.getLogger(getClass.getName)
//
//  def load(sparkSession: SparkSession, config: Config, currentDate: String): DataFrame = {
//
//    val basePath = config.getString("data_provisioning.data.output.kantar.active_user_aggregator.path")
//    val partition = config.getString("data_provisioning.data.output.kantar.active_user_aggregator.partition")
//    val format = config.getString("data_provisioning.data.output.kantar.active_user_aggregator.format")
//    val goBackDays = config.getInt("data_provisioning.data.input.shifu.inventory.back_days")
//
//    val finalStartDate = NCCSCommonUtils.getForwardOrBackWardDates(currentDate, -1 * goBackDays)
//    val finalEndDate = NCCSCommonUtils.getForwardOrBackWardDates(currentDate, -1 * 1)
//
//    val tier_1_List = config.getStringList("data_provisioning.data.input.shifu.inventory.tier1_list").asScala.toList
//    val tier_2_List = config.getStringList("data_provisioning.data.input.shifu.inventory.tier2_list").asScala.toList
//    val svod_list = config.getStringList("data_provisioning.data.input.shifu.inventory.svod_list").asScala.toList
//
//    val allPaths = ListBuffer[String]()
//    val dateRangeList = NCCSCommonUtils.getDatesList(finalStartDate, finalEndDate)
//    for (currentDate <- dateRangeList) {
//      allPaths += s"$basePath/$partition=$currentDate"
//    }
//
//    var masterData = sparkSession
//      .read
//      .option("basePath", basePath)
//      .format(format)
//      .load(allPaths: _*)
//      .withColumn("tier1", when(col("city").isin(tier_1_List: _*), true)
//        .otherwise(false))
//      .withColumn("tier2", when(col("city").isin(tier_2_List: _*), true)
//        .otherwise(false))
//      .withColumn("subscription_type", when(col("subscription_plan").isin(svod_list: _*), "SVOD")
//        .when(lower(col("subscription_plan")).like("%adsfree") || lower(col("subscription_plan")).like("%premium"), "Premium")
//        .otherwise("Others"))
//      .dropDuplicates("dw_p_id") //confirm if this is required
//
//    val default_age_bucket = NCCSCommonUtils.getMaxCountbucket(masterData, "age_bucket", List.empty[String])
//    val default_subscription_type = NCCSCommonUtils.getMaxCountbucket(masterData, "subscription_type", List.empty[String])
//
//    masterData = masterData.withColumn("age_bucket", when(col("age_bucket") === "Others", lit(default_age_bucket)).otherwise(col("age_bucket")))
//      .withColumn("0_19", when(col("age_bucket") === "0_19", lit(1)).otherwise(lit(0)))
//      .withColumn("20_24", when(col("age_bucket") === "20_24", lit(1)).otherwise(lit(0)))
//      .withColumn("25_P", when(col("age_bucket") === "25_P", lit(1)).otherwise(lit(0)))
//      .withColumn("MALE", when(col("gender") === "MALE", lit(1)).otherwise(lit(0)))
//      .withColumn("subscription_type", when(col("subscription_type") === "Others", lit(default_subscription_type)).otherwise(col("subscription_type")))
//      .withColumn("subscription_plan_svod", when(col("subscription_type") === "SVOD", lit(1)).otherwise(lit(0)))
//      .withColumn("subscription_plan_premium", when(col("subscription_type") === "Premium", lit(1)).otherwise(lit(0)))
//      .filter(col("age_bucket").isNotNull)
//      .select("0_19", "20_24", "25_P", "MALE", "subscription_plan_premium", "subscription_plan_svod", "dw_p_id", "tier1", "tier2", "city", "state", "cd", "hr")
//
//    print("count of device_profile: --->  " + masterData.count + "\n")
//    masterData
//  }
//
//  def transform(sparkSession: SparkSession, config: Config, dfInput: DataFrame, currentDate: String): DataFrame = {
//
//    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
//    val startDate = currentDate
//
//    val startDateTime = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(startDate)
//    val nDaysAgo = startDateTime.minusDays(360); //do we need to do one year back?
//    val nDaysAgoTimestamp = nDaysAgo.getMillis() / 1000;
//    val receivedAtFilter = "received_at >= " + nDaysAgoTimestamp.toString
//
//    val dfMasterDeviceProfile = dfInput
//
//    //Extract Device meta
//    val dfDeviceMeta = getDeviceMetaAsDataFrame(sparkSession, config, receivedAtFilter)
//
//    //Join Device meta
//    var dfEnrichedDeviceInfo = dfDeviceMeta.join(dfMasterDeviceProfile, Seq("dw_p_id"), "inner")
//      .withColumn("devices", count("*").over(Window.partitionBy(col("dw_p_id"))))
//      .dropDuplicates("dw_p_id")
//    print("count after joining deviceMeta =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Um users
//    val dfUmUsers = getUmUsersDataFrame(sparkSession, config, "in")
//
//    //Join Um Users
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfUmUsers, Seq("dw_p_id"), "left")
//      .select(
//        dfEnrichedDeviceInfo.col("*"),
//        coalesce(dfUmUsers.col("phone_no"), dfEnrichedDeviceInfo.col("dw_p_id")).as("phone_no")
//      )
//    print("count after joining Umusers =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Removing already calculated NCCCS
//    val dfAlreadyExistingNccsIDs = getExistingNCCSIdInfo(sparkSession, config, startDate)
//
//    // Remove existing NCCS phone_id's
//    dfAlreadyExistingNccsIDs match {
//      case Failure(exception) => {
//        throw new NoSuchElementException(s"Not able to detect any existing NCCS calculated phone_no ids, ${exception.getCause}")
//      }
//      case Success(existingPhoneIdsDf) => {
//        logger.info(s"Already existing NCCS calculated phone_no ids")
//        dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(existingPhoneIdsDf, Seq("phone_no"), "left_anti")
//      }
//    }
//    print("count after removing existing nccs phone ids =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    // df Brands
//    val manufacturer_list = config.getStringList("data_provisioning.data.input.kantar.brand_code_mapping.manufacturer_list").asScala.toList
//    var dfBrands = readDataFrame(sparkSession, config, "input", "brand_code_mapping", "", "")
//      .withColumnRenamed("Brand", "manufacturer")
//      .withColumnRenamed("Code", "Brand")
//
//    val max_manufacturer = NCCSCommonUtils.getMaxCountbucket(dfBrands, "manufacturer", manufacturer_list)
//
//    dfBrands = dfBrands.withColumn("manufacturer",
//      when(col("manufacturer").isin(manufacturer_list: _*), col("manufacturer"))
//        .otherwise(lit(max_manufacturer)))
//      .withColumn("Oppo", when(lower(col("manufacturer")).like("oppo%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Samsung", when(lower(col("manufacturer")).like("samsung%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Realme", when(lower(col("manufacturer")).like("realme%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Vivo", when(lower(col("manufacturer")).like("vivo%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Xiaomi", when(lower(col("manufacturer")).like("xiaomi%"), lit(1)).otherwise(lit(0)))
//
//    //Join dfBrands --> B99 brand refers to manufacturer Other, so for all the non matching rows we can provide B99
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfBrands,
//      lower(dfBrands("manufacturer")) <=> lower(dfEnrichedDeviceInfo("manufacturer")), "left")
//      .select(dfEnrichedDeviceInfo.col("*"), dfBrands.col("Brand"), dfBrands.col("Oppo"), dfBrands.col("Samsung"), dfBrands.col("Realme"), dfBrands.col("Vivo"), dfBrands.col("Xiaomi"))
//      .withColumn("Brand", when(col("Brand").isNull, lit("B99")).otherwise(col("Brand")))
//    print("count after joining dfBrands =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Device Price
//    var dfDevicePrice = readDataFrame(sparkSession, config, "input", "device_price_mapping", "", "")
//      .withColumnRenamed("_col0", "model")
//      .withColumnRenamed("_col1", "price_range")
//      .withColumn("Price", UserDefinedFunctions.getKantarDevicePrice(col("price_range")))
//
//    val price_list = config.getStringList("data_provisioning.data.input.kantar.device_price_mapping.price_list").asScala.toList
//    val max_price_bucket = NCCSCommonUtils.getMaxCountbucket(dfDevicePrice, "price_range", price_list)
//
//    dfDevicePrice = dfDevicePrice.withColumn("price_range", when(col("price_range").isin(price_list: _*), col("price_range"))
//      .otherwise(lit(max_price_bucket)))
//      .withColumn("15K_20K", when(col("price_range") === "15K-20K", lit(1)).otherwise(lit(0)))
//      .withColumn("25K_35K", when(col("price_range") === "25K-35K", lit(1)).otherwise(lit(0)))
//      .withColumn("10K_15K", when(col("price_range") === "10K-15K", lit(1)).otherwise(lit(0)))
//      .withColumn("20K_25K", when(col("price_range") === "20K-25K", lit(1)).otherwise(lit(0)))
//      .withColumn("35KPlus", when(col("price_range") === "35K+", lit(1)).otherwise(lit(0)))
//
//    //Join Device Price
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfDevicePrice,
//      lower(dfDevicePrice("model")) <=> lower(dfEnrichedDeviceInfo("model")), "inner")
//      .select(
//        dfEnrichedDeviceInfo.col("*"),
//        dfDevicePrice.col("Price"), dfDevicePrice.col("15K_20K"), dfDevicePrice.col("25K_35K"), dfDevicePrice.col("10K_15K"), dfDevicePrice.col("20K_25K"), dfDevicePrice.col("35KPlus")
//      )
//    print("count after joining dfDevice Price =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Apps Info and affluent apps
//    val dfAppsInfo = getNumberOfApps(sparkSession, config, startDate)
//
//    //Join App Info
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfAppsInfo, Seq("dw_p_id"), "left")
//      .select(dfEnrichedDeviceInfo.col("*"), dfAppsInfo.col("Apps"), dfAppsInfo.col("high_affluent_apps"), dfAppsInfo.col("low_affluent_apps"))
//
//    val apps_avg_no = dfEnrichedDeviceInfo
//      .filter(col("Apps").isNotNull)
//      .agg(avg("Apps").as("avg_apps")).head.getDouble(0).toInt
//
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.withColumn("Apps", coalesce(col("Apps"), lit(apps_avg_no)))
//    print("count after joining Apps info =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //package Columns
//    val dfPackageInfo = getNumberOfPackage(sparkSession, config, startDate)
//
//    //join the package info
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfPackageInfo, Seq("dw_p_id"), "left")
//    //      .select(dfEnrichedDeviceInfo.col("*"), dfPackageInfo.col("*"))
//    print(dfEnrichedDeviceInfo.printSchema())
//    print("count after joining package info =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Carrier Info
//    var dfCarrierInfo = getCarrierInfo(sparkSession, config, startDate)
//    val default_sim = NCCSCommonUtils.getMaxCountbucket(dfCarrierInfo, "SIM", List.empty[String])
//
//    dfCarrierInfo = dfCarrierInfo
//      .withColumn("SIM", when(col("SIM") === "Others", default_sim).otherwise(col("SIM")))
//      .withColumn("Airtel", when(lower(col("SIM")).like("airtel%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Jio", when(lower(col("SIM")).like("jio%"), lit(1)).otherwise(lit(0)))
//      .withColumn("Vodafone_Idea", when(lower(col("SIM")).like("vodafone - idea%"), lit(1)).otherwise(lit(0)))
//
//
//    //Join Carrier Info
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.join(dfCarrierInfo, Seq("dw_p_id"), "left")
//      .select(dfEnrichedDeviceInfo.col("*"),
//        dfCarrierInfo.col("Airtel"), dfCarrierInfo.col("Jio"), dfCarrierInfo.col("Vodafone_Idea"))
//    print("count after joining carrier info =  " + dfEnrichedDeviceInfo.count + "\n")
//
//
//    //Watch Video for 90 days and 30 days
//    val dfWatchVideo = getWatchVideoInfo(sparkSession, config, startDate, 90)
//    //    val dfWatchVideo_30 = getWatchVideoInfo(sparkSession, config, startDate, 30)
//
//    //join watch video_30
//    //    val dfEnrichedDeviceInfo_30 = dfWatchVideo_30.join(dfEnrichedDeviceInfo, Seq("dw_p_id"), "inner")
//    //Join Watch video_90
//    dfEnrichedDeviceInfo = dfWatchVideo.join(dfEnrichedDeviceInfo, Seq("dw_p_id"), "inner")
//    //    print("watch video join 30 days completed " + dfEnrichedDeviceInfo_30.count + "\n")
//    //    print("watch video join completed " + dfEnrichedDeviceInfo.count + "\n")
//
//    //Network Type
//    dfEnrichedDeviceInfo = dfEnrichedDeviceInfo.withColumn("carrier_type", UserDefinedFunctions.getCarrierType(col("network_type")))
//      .withColumn("4G", when(col("carrier_type") === "4G", lit(1)).otherwise(lit(0)))
//
//    // persisting  dfEnrichedDeviceInfo as its being used multiple times
//    dfEnrichedDeviceInfo.persist(StorageLevel.MEMORY_AND_DISK)
//
//
//    // average watch time calculation
//    var dfAverageWatchTime = dfEnrichedDeviceInfo.select("phone_no", "watch_time", "watch_date")
//    dfAverageWatchTime = avgWatchTime(dfAverageWatchTime, "", List(""), "avg_time");
//
//    print(dfAverageWatchTime.printSchema())
//
//    //platform avg watch time
//    val platformAvgWatchTime_90 = getPlatformAvgWatchTime(dfEnrichedDeviceInfo, config, "90")
//    //    val platformAvgWatchTime_30 = getPlatformAvgWatchTime(dfEnrichedDeviceInfo_30, config, "30")
//    val dfEngAvgWatchTime_90 = getAvgWatchTime(dfEnrichedDeviceInfo, List("english"), "language", "eng_avg_time_90")
//    //    val dfEngAvgWatchTime_30 = getAvgWatchTime(dfEnrichedDeviceInfo_30, List("english"), "language", "eng_avg_time_30")
//
//    val sportsEnrichedDeviceInfo = dfEnrichedDeviceInfo.filter(lower(col("content_type")).isin("sport_match_highlights", "sport_live", "sport", "sport_replay", "sports_video_feed"))
//    //    val sportsEnrichedDeviceInfo_30 = dfEnrichedDeviceInfo_30.filter(lower(col("content_type")).isin("sport_match_highlights", "sport_live", "sport", "sport_replay", "sports_video_feed"))
//    val sportsAvgWatchTime_90 = getSportsAvgWatchTime(sportsEnrichedDeviceInfo, config, "90")
//    //    val sportsAvgWatchTime_30 = getSportsAvgWatchTime(sportsEnrichedDeviceInfo_30, config, "30")
//
//
//    // Content Type Transformation
//    val dfViewershipDetailsTransformed = dfEnrichedDeviceInfo.select(
//      col("phone_no"),
//      col("content_type"),
//      col("language"),
//      col("channel")
//    ).dropDuplicates()
//
//    val dfContentTypeDetailsTransformed = dfViewershipDetailsTransformed.select(col("*"), when(lower(col("language")) === "english", "English")
//      .when(lower(col("language")) === "hindi", "Hindi")
//      .otherwise("Regional").alias("refined_language"))
//      .withColumn("Webseries", when(lower(col("channel")).isin("hotstar originals", "hotstar specials"), 1).otherwise(0))
//      .withColumn("Sports", when(lower(col("content_type")).isin("sport_match_highlights", "sport_live", "sport", "sport_replay", "sports_video_feed"), 1).otherwise(0))
//      .withColumn("Hindi|Movies", when(lower(col("refined_language")) === "hindi" && lower(col("content_type")).isin("movie"), 1).otherwise(0))
//      .withColumn("Hindi|TVShows", when(lower(col("refined_language")) === "hindi" && lower(col("content_type")).isin("episode", "series"), 1).otherwise(0))
//      .withColumn("English|Movies", when(lower(col("refined_language")) === "english" && lower(col("content_type")).isin("movie"), 1).otherwise(0))
//      .withColumn("English|TVShows", when(lower(col("refined_language")) === "english" && lower(col("content_type")).isin("episode", "series"), 1).otherwise(0))
//      .withColumn("Regional|Movies", when(lower(col("refined_language")) === "regional" && lower(col("content_type")).isin("movie"), 1).otherwise(0))
//      .withColumn("Regional|TVShows", when(lower(col("refined_language")) === "regional" && lower(col("content_type")).isin("episode", "series"), 1).otherwise(0))
//
//    val contentTypeList = Seq("Hindi|Movies", "Hindi|TVShows", "English|Movies", "English|TVShows", "Regional|Movies", "Regional|TVShows", "Sports", "Webseries")
//    var dfContentTypePivot = dfContentTypeDetailsTransformed.groupBy("phone_no").agg(
//      sum(col("Hindi|Movies")).as("Hindi|Movies"),
//      sum(col("Hindi|TVShows")).as("Hindi|TVShows"),
//      sum(col("English|Movies")).as("English|Movies"),
//      sum(col("English|TVShows")).as("English|TVShows"),
//      sum(col("Regional|Movies")).as("Regional|Movies"),
//      sum(col("Regional|TVShows")).as("Regional|TVShows"),
//      sum(col("Webseries")).as("Webseries"),
//      sum(col("Sports")).as("Sports")
//    )
//
//    for (column <- contentTypeList) {
//      dfContentTypePivot = dfContentTypePivot.withColumn(column, when(col(column) >= 1, "Yes").otherwise("No"))
//    }
//
//    // merging all the data parts
//    logger.info("[INFO] merging all the data parts device + avg watch time + genre")
//    val dfModelDeviceInfo = dfEnrichedDeviceInfo.select("phone_no", "Apps", "devices", "Brand", "Price", "city", "state", "tier1", "tier2", "high_affluent_apps", "low_affluent_apps", "banking", "investing_trading", "wallets", "international_OTT_Premium", "indian_OTT_Premium",
//      "international_OTT_Non_Premium", "indian_OTT_Non_Premium", "deals_Coupons", "games", "food_ordering",
//      "online_pharmacy_medical_consultation", "quick_grocery", "travel", "Oppo", "Samsung", "Realme",
//      "Vivo", "Xiaomi", "15K_20K", "25K_35K", "10K_15K", "20K_25K", "35KPlus", "0_19", "20_24", "25_P", "Jio", "Airtel", "Vodafone_Idea",
//      "MALE", "subscription_plan_svod", "subscription_plan_premium","4G", "cd", "hr").dropDuplicates("phone_no")
//
//
//    // un persisting dfEnrichedDeviceInfo
//    dfEnrichedDeviceInfo.unpersist()
//
//    //Joining the enriched info with content_type
//    var dfKantarProfile = dfModelDeviceInfo.join(dfContentTypePivot, Seq("phone_no"), "inner") //think about window partition
//      .join(dfAverageWatchTime, Seq("phone_no"), "inner")
//      .join(platformAvgWatchTime_90, Seq("phone_no"), "inner")
//      .join(dfEngAvgWatchTime_90, Seq("phone_no"), "left")
//      .join(sportsAvgWatchTime_90, Seq("phone_no"), "left")
//      .filter(col("phone_no").isNotNull)
//      .filter(col("Apps").isNotNull)
//      .filter(col("Brand").isNotNull)
//      .filter(col("Price").isNotNull)
//      .dropDuplicates("phone_no")
//
//    print(dfKantarProfile.printSchema())
//
//    //Update column names
//    dfKantarProfile = updateColumnNames(dfKantarProfile)
//    print("final logs : " + dfKantarProfile.count + "\n")
//
//
//    // adding dummy record to avoid failure of kantar model in case all columns contain value No
//    //    val dummyRecord1 = sparkSession.createDataFrame(Seq(("dummyrecord000000000000001111111111122222222dummyrecocrd21323224",
//    //      1, "B1", 0.01, 1, 1, 1, "4G", 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, "Yes"),
//    //      ("dummyrecord000000000000001111111111122222222dummyrecocrd21323225",
//    //        1, "B1", 0.01, 1, 1, 1, "5G", 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, "No")))
//    //      .toDF("PhoneNo", "Apps", "Brand", "Avg_time_Spent_in_hrs", "Devices", "low_affluent_apps", "high_affluent_apps", "CarrierType", "Avg_time_spent_on_CTV_90_days_in_hrs", "Avg_time_spent_on_Mobile_90_days_in_hrs", "Avg_time_spent_on_Other_90_days_in_hrs", "Avg_time_spent_on_Eng_90_days_in_hrs", "Avg_time_spent_on_Sports_Eng_90_days_in_hrs", "Avg_time_spent_on_Sports_NonEng_90_days_in_hrs", "English.Movies")
//    //
//    //    val dummyRecord2 = sparkSession.createDataFrame(Seq(("dummyrecord000000000000001111111111122222222dummyrecocrd21323224",
//    //      "Yes", "Yes", "Yes", "Yes", "Yes", 10, "Yes", "Yes", "20_24", "Jio", "SUBSCRIBED_FREE_LOGGED_IN", "Male", true, false, "MUMBAI", "MH", "2023-10-30", 10),
//    //      ("dummyrecord000000000000001111111111122222222dummyrecocrd21323225",
//    //        "No", "No", "No", "No", "No", 10, "No", "No", "0_19", "Jio", "SUBSCRIBED_FREE_LOGGED_IN", "Female", false, true, "PUNE", "MH", "2023-10-30", 10)))
//    //      .toDF("PhoneNo", "English.TV_Shows", "Hindi.Movies", "Hindi.TV_Shows", "Regional.Movies", "Regional.TV_Shows", "Price", "Sports", "Web_series", "Age", "SIM", "Subscription_Plan", "Gender", "Tier1", "Tier2", "City", "State", "cd", "hr")
//    //    val dummyRecord = dummyRecord1.join(dummyRecord2, Seq("PhoneNo"), "inner")
//
//    //    dfKantarProfile = dfKantarProfile.union(dummyRecord)
//    dfKantarProfile = dfKantarProfile.withColumn("date_ist", lit(startDate))
//    //    print(dfKantarProfile.printSchema())
//
//    dfKantarProfile
//  }
//
//  def write(sparkSession: SparkSession, config: Config, outputDf: DataFrame): Unit = {
//
//    val format = config.getString(s"data_provisioning.data.output.kantar.kantar_profile_coalesced.format")
//    val path = config.getString(s"data_provisioning.data.output.kantar.kantar_profile_coalesced.output")
//    val write_mode = config.getString(s"data_provisioning.data.output.kantar.kantar_profile_coalesced.write_mode")
//    val compression = config.getString(s"data_provisioning.data.output.kantar.kantar_profile_coalesced.compression")
//    val partition = config.getString(s"data_provisioning.data.output.kantar.kantar_profile_coalesced.partition")
//
//    outputDf.coalesce(20)
//      .write
//      .format(format)
//      .mode(write_mode)
//      .partitionBy(partition)
//      .option("compression", compression)
//      .option("header", "true")
//      .save(path)
//  }
//
//  private def getDeviceMetaAsDataFrame(sparkSession: SparkSession, config: Config, receivedAtFilter: String): DataFrame = {
//    val deviceMetaTable = config.getString("data_provisioning.data.input.device_meta_snapshot_state_files.table")
//
//    sparkSession.sql(s"select device_id, pid, adv_platform_id, advertisingid, " +
//      s"dw_d_id, dw_p_id, platform, received_at, country, name, manufacturer, model from ${deviceMetaTable}")
//      .select(
//        col("dw_p_id"),
//        col("dw_d_id"),
//        col("received_at"),
//        col("manufacturer"),
//        col("model"))
//      .filter(col("model").isNotNull)
//      .filter(receivedAtFilter)
//      .select("dw_p_id", "manufacturer", "model", "dw_d_id")
//  }
//
//  private def getUmUsersDataFrame(sparkSession: SparkSession, config: Config, country: String): DataFrame = {
//    val salt = config.getString(s"data_provisioning.data.input.kantar.salt")
//    val umUsersTable = config.getString(s"data_provisioning.data.input.um_users_snapshot_state_file.$country-table")
//    // UM Users for Phone number
//    val dfUmUsers = sparkSession.sql(s"select key, subs, timestamp, acnlastupdatedon, extauthid, partners, lastupdatedon, userstatus, " +
//      s"signupdeviceid, lname, phonenumber, ispasswordrehashed, fname, email, subscriptions, joinedon, signupcountrycode, " +
//      s"usertype, isemailverified, isuserconsentgiven, fblastupdatedon, gender, subsdetails, pid, isphoneverified, hid, age, " +
//      s"signupplatform, version, profiles, is_deleted from ${umUsersTable}")
//      .select(
//        col("pid"),
//        col("phonenumber"))
//      .filter(col("pid").isNotNull)
//      .filter(col("phonenumber").isNotNull)
//      .withColumn("phone_no", UserDefinedFunctions.sha256WithSalt(col("phonenumber"), lit(salt)))
//      .withColumn("dw_p_id", UserDefinedFunctions.sha256(col("pid")))
//      .select("phone_no", "dw_p_id")
//    dfUmUsers
//  }
//
//  private def getExistingNCCSIdInfo(sparkSession: SparkSession, config: Config, startDate: String): Try[DataFrame] = {
//
//    val format = config.getString(s"data_provisioning.data.input.kantar.nccs_phone_mapping.format")
//    val basePath = config.getString(s"data_provisioning.data.input.kantar.nccs_phone_mapping.path")
//    val goBackDays = config.getInt(s"data_provisioning.data.input.kantar.nccs_phone_mapping.go_back_days")
//    val partition = config.getString(s"data_provisioning.data.input.kantar.nccs_phone_mapping.partition")
//
//    val startDateOverride = NCCSCommonUtils.getForwardOrBackWardDates(startDate, -1 * goBackDays)
//    val endDate = startDate
//    val bucket = NCCSCommonUtils.getBucket(basePath)
//    val allPaths = ListBuffer[String]()
//    val dateRangeList = NCCSCommonUtils.getDatesList(startDateOverride, endDate)
//    for (currentDate <- dateRangeList) {
//      val currentPath = s"$basePath$partition=$currentDate"
//      if (FileSystem.get(new URI(bucket), sparkSession.sparkContext.hadoopConfiguration).exists(new Path(currentPath))) {
//        allPaths.append(currentPath)
//      }
//    }
//
//    Try {
//      sparkSession.read
//        .option("header", true)
//        .option("basePath", basePath)
//        .format(format)
//        .load(allPaths: _*)
//        .withColumnRenamed("Phone No.", "phone_no")
//        .select("phone_no")
//    }
//  }
//
//  private def readDataFrame(sparkSession: SparkSession, config: Config, root_key: String, key: String, partition: String, endDate: String): DataFrame = {
//    val basePath = config.getString(s"data_provisioning.data.$root_key.kantar.$key.path")
//    val format = config.getString(s"data_provisioning.data.$root_key.kantar.$key.format")
//
//    var path = basePath
//    println(path)
//    if (partition != null && partition.nonEmpty) {
//      path = s"$basePath$partition=$endDate"
//      sparkSession
//        .read
//        .option("basePath", basePath)
//        .option("header", "true")
//        .format(format)
//        .load(path)
//    } else {
//      sparkSession
//        .read
//        .option("header", "true")
//        .format(format)
//        .load(path)
//    }
//  }
//
//  private def getNumberOfApps(sparkSession: SparkSession, config: Config, startDate: String): DataFrame = {
//    val format = config.getString(s"data_provisioning.data.input.kantar.apps_mapping.format")
//    val path = config.getString(s"data_provisioning.data.input.kantar.apps_mapping.path").replaceAll("start_date", startDate)
//    val high_apps_list = sparkSession.sql("select * from adtech.high_affluent_apps_aa_an")
//      .select("apps").distinct().collect().map(_.getString(0))
//    val low_apps_list = sparkSession.sql("select * from adtech.low_affluent_apps_aa_an")
//      .select("apps").distinct().collect().map(_.getString(0))
//
//    val dfNumberOfApps = sparkSession.read.format(format).load(path)
//      .select(col("dw_p_id"), explode(map_keys(col("feature"))) as "feature")
//      .withColumn("feature", regexp_replace(col("feature"), "uia-appName-", ""))
//      .groupBy("dw_p_id")
//      .agg(
//        sum(when(low_apps_list.map(userLowApp => col("feature").contains(userLowApp)).reduce(_ or _), 1).otherwise(0)).alias("low_affluent_apps"),
//        sum(when(high_apps_list.map(userHighApp => col("feature").contains(userHighApp)).reduce(_ or _), 1).otherwise(0)).alias("high_affluent_apps"),
//        count(col("feature")).alias("Apps")
//      )
//      .dropDuplicates("dw_p_id")
//    dfNumberOfApps
//  }
//
//  private def getNumberOfPackage(sparkSession: SparkSession, config: Config, startDate: String): DataFrame = {
//    val format = config.getString(s"data_provisioning.data.input.kantar.package_mapping.format")
//    val path = config.getString(s"data_provisioning.data.input.kantar.package_mapping.path").replaceAll("start_date", startDate)
//    val packageColumnsList = config.getStringList(s"data_provisioning.data.input.kantar.package_mapping.package_column_list").asScala.toList
//    val packageColPath = "data_provisioning.data.input.kantar.package_mapping."
//
//    var dfPackage = sparkSession.read.format(format).load(path)
//      .select(col("dw_p_id"), explode(map_keys(col("feature"))) as "feature")
//      .withColumn("feature", regexp_replace(col("feature"), "uia-app_package-", ""))
//
//    for (packageColumn <- packageColumnsList) {
//      val package_list = config.getStringList(packageColPath + packageColumn).asScala.toList
//      dfPackage = dfPackage.withColumn(packageColumn, when(package_list.map(userPackage => col("feature").contains(userPackage)).reduce(_ or _), 1).otherwise(0))
//    }
//
//    dfPackage
//      .groupBy("dw_p_id")
//      .agg(
//        sum(col("banking")).alias("banking"),
//        sum(col("investing_trading")).alias("investing_trading"),
//        sum(col("wallets")).alias("wallets"),
//        sum(col("international_OTT_Premium")).alias("international_OTT_Premium"),
//        sum(col("indian_OTT_Premium")).alias("indian_OTT_Premium"),
//        sum(col("international_OTT_Non_Premium")).alias("international_OTT_Non_Premium"),
//        sum(col("indian_OTT_Non_Premium")).alias("indian_OTT_Non_Premium"),
//        sum(col("deals_Coupons")).alias("deals_Coupons"),
//        sum(col("games")).alias("games"),
//        sum(col("food_ordering")).alias("food_ordering"),
//        sum(col("online_pharmacy_medical_consultation")).alias("online_pharmacy_medical_consultation"),
//        sum(col("quick_grocery")).alias("quick_grocery"),
//        sum(col("travel")).alias("travel")
//      ).dropDuplicates("dw_p_id")
//  }
//
//  private def getCarrierInfo(sparkSession: SparkSession, config: Config, startDate: String): DataFrame = {
//    val userprofileSnapshotTableName = config.getString(s"data_provisioning.data.input.kantar.carrier_mapping.user_profile_snapshot_table")
//    val goBackDays = config.getString(s"data_provisioning.data.input.kantar.carrier_mapping.go_back_days")
//
//    val dfCarrierInfo = sparkSession.sql(
//      s"""select dw_p_id, last_network_carrier, last_updated_on from ${userprofileSnapshotTableName}
//         | where last_network_carrier is not null and
//         | last_updated_on >= ( to_date('${startDate}', "yyyy-MM-dd") - INTERVAL ${goBackDays} DAYS)
//         |""".stripMargin)
//      .withColumn("SIM", UserDefinedFunctions.getKantarSimBucket(col("last_network_carrier")))
//      .select("dw_p_id", "SIM")
//    dfCarrierInfo
//  }
//
//  private def getWatchVideoInfo(sparkSession: SparkSession, config: Config, startDate: String, goBackDays: Int): DataFrame = {
//    val watch_video_aggr_table = config.getString(s"data_provisioning.data.input.kantar.watch_video_aggr.watch_video_aggr_table")
//    val goForwardDays = 0
//
//    val finalStartDate = NCCSCommonUtils.getForwardOrBackWardDates(startDate, -1 * goBackDays)
//    val finalEndDate = NCCSCommonUtils.getForwardOrBackWardDates(startDate, goForwardDays)
//
//    val dfWatchData = sparkSession.sql(s"""select dw_p_id, platform, network_type, watch_time, language, cms_channel_name as channel, cms_content_type as content_type, max_ts from $watch_video_aggr_table where cd >= to_date('$finalStartDate', 'yyyy-MM-dd') and cd <= to_date('$finalEndDate', 'yyyy-MM-dd') """)
//      .filter(col("dw_p_id").isNotNull).filter(col("dw_p_id") =!= lit(""))
//      .withColumn("watch_date", to_date(from_utc_timestamp(col("max_ts"), "IST"), "yyyy-MM-dd"))
//      .drop(col("max_ts"))
//
//    dfWatchData
//  }
//
//  private def getAvgWatchTime(data: DataFrame, featureList: List[String], column: String, columnName: String): DataFrame = {
//
//    val dfFeatureWatchData = data.select(
//      col("phone_no"),
//      col("watch_time"),
//      col("watch_date"),
//      col(column)
//    ).filter(col(column).isNotNull)
//      .filter(lower(col(column)).isin(featureList: _*))
//
//    avgWatchTime(dfFeatureWatchData, "", List(""), columnName)
//  }
//
//  private def getPlatformAvgWatchTime(data: DataFrame, config: Config, days: String): DataFrame = {
//    val ctvList = config.getStringList("data_provisioning.data.input.kantar.watch_video_aggr.platform.ctv_list").asScala.toList
//    val mobileList = config.getStringList("data_provisioning.data.input.kantar.watch_video_aggr.platform.mobile_list").asScala.toList
//    val categoryPath = "data_provisioning.data.input.kantar.watch_video_aggr.platform." + days + ".pltCategories"
//    val pltCategories = config.getStringList(categoryPath).asScala.toList
//
//    //dividing the platform as ctv, mobile and others.
//    val dfPlatformWatchData = data.select(
//      col("phone_no"),
//      col("watch_time"),
//      col("watch_date"),
//      col("platform")
//    ).filter(col("platform").isNotNull)
//      .withColumn("platformCategory", when(lower(col("platform")).isin(ctvList: _*), pltCategories(0))
//        .when(lower(col("platform")).isin(mobileList: _*), pltCategories(1))
//        .otherwise(pltCategories(2)))
//
//    avgWatchTime(dfPlatformWatchData, "platformCategory", pltCategories, "")
//  }
//
//  private def getSportsAvgWatchTime(data: DataFrame, config: Config, days: String): DataFrame = {
//    val lanList = config.getStringList("data_provisioning.data.input.kantar.watch_video_aggr.sports.language_list").asScala.toList
//    val categoryPath = "data_provisioning.data.input.kantar.watch_video_aggr.sports." + days + ".lanCategories"
//    val lanCategories = config.getStringList(categoryPath).asScala.toList
//
//    //dividing the platform as ctv, mobile and others.
//    val dfSportsWatchData = data.select(
//      col("phone_no"),
//      col("watch_time"),
//      col("watch_date"),
//      col("language")
//    ).filter(col("language").isNotNull)
//      .withColumn("languageCategory", when(lower(col("language")).isin(lanList: _*), lanCategories(0))
//        .otherwise(lanCategories(1)))
//
//    avgWatchTime(dfSportsWatchData, "languageCategory", lanCategories, "")
//  }
//
//  private def avgWatchTime(dfWatchData: DataFrame, categoryColumn: String, categoryList: List[String], columnName: String): DataFrame = {
//    if (categoryColumn == "") {
//      return dfWatchData
//        .groupBy("phone_no", "watch_date")
//        .agg(
//          sum("watch_time").as("total_watch_time_daily")
//        ).groupBy("phone_no")
//        .agg(
//          avg("total_watch_time_daily").as("avg_watch_time")
//        ).withColumn("avg_time", col("avg_watch_time") / 60)
//        .withColumnRenamed("avg_time", columnName)
//        .select("phone_no", columnName).na.fill(0)
//    }
//    dfWatchData
//      .groupBy("phone_no", categoryColumn, "watch_date")
//      .agg(
//        sum("watch_time").as("total_watch_time_daily")
//      ).groupBy("phone_no", categoryColumn)
//      .agg(
//        avg("total_watch_time_daily").as("avg_watch_time")
//      ).withColumn("avg_time", col("avg_watch_time") / 60)
//      .select("phone_no", "avg_time", categoryColumn)
//      .groupBy("phone_no").pivot(categoryColumn, categoryList).sum("avg_time").na.fill(0)
//  }
//
//
//  private def updateColumnNames(dfKantarProfile: DataFrame): DataFrame = {
//    dfKantarProfile.select("phone_no",
//      "Apps",
//      "Brand",
//      "avg_time",
//      "devices",
//      "low_affluent_apps",
//      "high_affluent_apps",
//      "carrier_type",
//      "ctv_avg_time_90",
//      "mobile_avg_time_90",
//      "other_avg_time_90",
//      "eng_avg_time_90",
//      "sports_eng_avg_time_90",
//      "sports_nonEng_avg_time_90",
//      "English|Movies",
//      "English|TVShows",
//      "Hindi|Movies",
//      "Hindi|TVShows",
//      "Regional|Movies",
//      "Regional|TVShows",
//      "Price",
//      "Sports",
//      "Webseries",
//      "Oppo", "Samsung", "Realme", "Vivo", "Xiaomi", "15K_20K", "25K_35K", "10K_15K", "20K_25K", "35KPlus", "0_19", "20_24",
//      "25_P", "Jio", "Airtel", "Vodafone_Idea", "MALE", "subscription_plan_svod", "subscription_plan_premium", "4G",
//      "tier1",
//      "tier2",
//      "city",
//      "state", "banking", "investing_trading", "wallets", "international_OTT_Premium", "indian_OTT_Premium",
//      "international_OTT_Non_Premium", "indian_OTT_Non_Premium", "deals_Coupons", "games", "food_ordering",
//      "online_pharmacy_medical_consultation", "quick_grocery", "travel",
//      "cd",
//      "hr"
//    ).withColumnRenamed("phone_no", "PhoneNo")
//      .withColumnRenamed("Apps", "Apps")
//      .withColumnRenamed("Brand", "Brand")
//      .withColumnRenamed("devices", "Devices")
//      .withColumnRenamed("low_affluent_apps", "low_affluent_apps")
//      .withColumnRenamed("high_affluent_apps", "high_affluent_apps")
//      .withColumnRenamed("avg_time", "Avg_time_Spent_in_hrs")
//      .withColumnRenamed("ctv_avg_time_90", "Avg_time_spent_on_CTV_90_days_in_hrs")
//      .withColumnRenamed("mobile_avg_time_90", "Avg_time_spent_on_Mobile_90_days_in_hrs")
//      .withColumnRenamed("other_avg_time_90", "Avg_time_spent_on_Other_90_days_in_hrs")
//      .withColumnRenamed("eng_avg_time_90", "Avg_time_spent_on_Eng_90_days_in_hrs")
//      .withColumnRenamed("sports_eng_avg_time_90", "Avg_time_spent_on_Sports_Eng_90_days_in_hrs")
//      .withColumnRenamed("sports_nonEng_avg_time_90", "Avg_time_spent_on_Sports_NonEng_90_days_in_hrs")
//      .withColumnRenamed("English|Movies", "English.Movies")
//      .withColumnRenamed("English|TVShows", "English.TV_Shows")
//      .withColumnRenamed("Hindi|Movies", "Hindi.Movies")
//      .withColumnRenamed("Hindi|TVShows", "Hindi.TV_Shows")
//      .withColumnRenamed("Regional|Movies", "Regional.Movies")
//      .withColumnRenamed("Regional|TVShows", "Regional.TV_Shows")
//      .withColumnRenamed("Price", "Price")
//      .withColumnRenamed("Sports", "Sports")
//      .withColumnRenamed("Webseries", "Web_series")
//      .withColumnRenamed("city", "City")
//      .withColumnRenamed("state", "State")
//      .withColumnRenamed("tier1", "Tier1")
//      .withColumnRenamed("tier2", "Tier2")
//      .withColumnRenamed("banking", "Banking")
//      .withColumnRenamed("investing_trading", "Investing / Trading")
//      .withColumnRenamed("wallets", "Wallets")
//      .withColumnRenamed("international_OTT_Premium", "International OTT (Premium)")
//      .withColumnRenamed("indian_OTT_Premium", "Indian OTT (Premium)")
//      .withColumnRenamed("international_OTT_Non_Premium", "International OTT (Non-Premium)")
//      .withColumnRenamed("indian_OTT_Non_Premium", "Indian OTT (Non-Premium)")
//      .withColumnRenamed("deals_Coupons", "Deals / Coupons")
//      .withColumnRenamed("games", "Games")
//      .withColumnRenamed("food_ordering", "Food ordering")
//      .withColumnRenamed("online_pharmacy_medical_consultation", "Online pharmacy / Medical consultation")
//      .withColumnRenamed("quick_grocery", "Quick Grocery")
//      .withColumnRenamed("travel", "Travel")
//      .withColumnRenamed("subscription_plan", "Subscription_Plan")
//      .withColumnRenamed("Oppo", "new_manufacturer_OPPO")
//      .withColumnRenamed("Samsung", "new_manufacturer_samsung")
//      .withColumnRenamed("Realme", "new_manufacturer_realme")
//      .withColumnRenamed("Xiaomi", "new_manufacturer_Xiaomi")
//      .withColumnRenamed("Vivo", "new_manufacturer_vivo")
//      .withColumnRenamed("10K_15K", "Price_10K-15K")
//      .withColumnRenamed("15K_20K", "Price_15K-20K")
//      .withColumnRenamed("20K_25K", "Price_20K-25K")
//      .withColumnRenamed("25K_35K", "Price_25K-35K")
//      .withColumnRenamed("35KPlus", "Price_35K+")
//      .withColumnRenamed("0_19", "Age_0_19")
//      .withColumnRenamed("20_24", "Age_20_24")
//      .withColumnRenamed("25_P", "Age_25_P")
//      .withColumnRenamed("Jio", "SIM_Jio")
//      .withColumnRenamed("Airtel", "SIM_Airtel")
//      .withColumnRenamed("Vodafone_Idea", "SIM_Vodafone - Idea")
//      .withColumnRenamed("MALE", "Gender_MALE")
//      .withColumnRenamed("subscription_plan_svod", "Subscription Plan_svod")
//      .withColumnRenamed("subscription_plan_premium", "Subscription Plan_premium")
//      .withColumnRenamed("4G", "Carrier Type_4G")
//  }
//}
//
//
//object KantarTransformDataWorkflow {
//
//  def main(args: Array[String]): Unit = {
//
//    val eventName = "nccs"
//    val config = ApplicationProperties.getConf(eventName)
//
//    val appName = config.getString("data_provisioning.application.app_name")
//    val sparkObjects = new SparkUtil(appName, config.getString("data_provisioning.application.log_level"))
//    val sparkSession = sparkObjects.getSparkSession
//    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
//
//    val cd = System.getProperty("cd")
//
//    val KantarTransformer = new KantarTransformDataWorkflow()
//    val dfInput = KantarTransformer.load(sparkSession, config, cd)
//    val dfOutput = KantarTransformer.transform(sparkSession, config, dfInput, cd)
//    KantarTransformer.write(sparkSession, config, dfOutput)
//
//    sparkSession.close()
//  }
//}
//
