import org.apache.spark.sql.DataFrame
import org.example.Driver
import org.example.data.loadData
import org.example.transform.Transform

class sparkEtl extends Driver {
    override def load(): List[DataFrame] = {
        val loadObj = new loadData()
        val dataset_1 = loadObj.loadDataset_1()
        val dataset_2 = loadObj.loadDataset_2()
        List(dataset_1,dataset_2)
    }

    override def transform(dataFrames: List[DataFrame]): DataFrame ={
        val transformObj = new Transform()
        println("transform")
        transformObj.aggregateFunctions(dataFrames)
    }

    override def write(finalDataset:DataFrame): Unit = {
        println("write func")
        val loadObj = new loadData()
        loadObj.writeDataframe(finalDataset, "finalDataset")
        finalDataset.show()
    }
}

object Main{

    def main(args: Array[String]): Unit = {
        val sparkEtl = new sparkEtl()
        val dataFrames: List[DataFrame] = sparkEtl.load()
        val finalDataset = sparkEtl.transform(dataFrames)
        sparkEtl.write(finalDataset)
    }

}