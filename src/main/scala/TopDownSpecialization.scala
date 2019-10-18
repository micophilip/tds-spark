import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TopDownSpecialization extends Serializable {

  def main(args: Array[String]) = {

    val inputPath = args(0)

    val QIDs = List("age", "education", "marital-status", "occupation", "native-country")

    val spark = SparkSession.builder().appName("TopDownSpecialization")
      .config("spark.master", "local").getOrCreate()
    val input = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(inputPath)

    val subset = input.select(QIDs.head, QIDs.tail: _*)

    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    subsetWithK.show()

    spark.stop()
  }

}
