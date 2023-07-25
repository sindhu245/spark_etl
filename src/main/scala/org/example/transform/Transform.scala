package org.example.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.time.LocalDateTime

class Transform {
    def aggregateFunctions(dataFrames: List[DataFrame]): DataFrame ={
      val dataset_1:DataFrame = dataFrames(0)
      val dataset_2: DataFrame = dataFrames(1)
      val current_time = LocalDateTime.now.toString

      val percentageValues = dataset_1.agg(expr("percentile(score, 0.9)").as("90_percentileScore"), expr("percentile(price, 0.95)").as("95_percentilePrice"))
      percentageValues.show()

      val dataset_3 = dataset_1.groupBy("genre").agg(avg(size(split(col("attributes"),"\t"))-1) as "avgAttributeCount", (sum(when(col("expiry")>current_time,0).otherwise(1))/count(col("expiry")>current_time))*100 as "percentageOfExpiredTags", avg(col("price")) as "avgCost")
      val finaData:DataFrame = dataset_3.join(dataset_2, dataset_3("genre") === dataset_2("genreId"), "inner")
      finaData
    }
}