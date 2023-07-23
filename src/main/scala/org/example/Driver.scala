package org.example

import org.apache.spark.sql.DataFrame

trait Driver {
      def load():List[DataFrame]
      def transform(dataFrames: List[DataFrame]):DataFrame
      def write(finalDataset:DataFrame)
}
