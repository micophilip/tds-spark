import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TopDownSpecialization extends Serializable {

  def main(args: Array[String]) = {

    val inputPath = args(0)

    val spark = SparkSession.builder().appName("TopDownSpecialization")
      .config("spark.master", "local").getOrCreate()
    val input = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(inputPath)

    input.show()

    spark.stop()
  }

}
