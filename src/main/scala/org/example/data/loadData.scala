package org.example.data

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class loadData{
  val sp = SparkSession.builder().appName("spark-ETL").master("local[*]").getOrCreate()
  sp.sparkContext.setLogLevel("ERROR")

    def loadDataset_1(): DataFrame = {
      //dataset 1
      val dataset_1 = new Dataset_1()
      val schema = Seq("id", "score", "price", "attributes", "timeStamp", "expiry", "genre")
//      writeDataframe(dataset_1.getDataset1(schema) ,"dataset_1")     //Write dataframe
      readDataframe("dataset_1") //read dataframe from mysql
//      dataset_1.getDataset1(schema)
    }

    def loadDataset_2(): DataFrame= {
        //dataset 2
      val dataset_2 = new Dataset_2()
      val schema = Seq("genreId", "genreName")
//      writeDataframe(dataset_2.getDataset2(schema),"dataset_2")   //Write dataframe
      readDataframe("dataset_2") //read dataframe from mysql
//     dataset_2.getDataset2(schema)
    }

    def writeDataframe(dataFrame: DataFrame, tableName: String): Unit = {
      dataFrame.write.mode(SaveMode.Overwrite)
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/newdb")
        .option("dbtable", tableName)
        .option("user", "root")
        .option("password", "root")
        .save()
    }

    def readDataframe(tableName: String): DataFrame = {
      val loadDataframe: DataFrame = sp.read
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/newdb")
        .option("dbtable", tableName)
        .option("user", "root")
        .option("password", "root")
        .load()

      loadDataframe
    }
}
