import argonaut.Argonaut._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.math._

import scala.io.Source

object TopDownSpecialization extends Serializable {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("TopDownSpecialization")
      .config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val inputPath = args(0)
    val taxonomyTreePath = args(1)
    val k = args(2)
    val sensitiveAttributeColumn = args(3)

    println(s"Anonymizing dataset in $inputPath")
    println(s"Running TDS with k = $k")

    val QIDs = List("age", "education", "marital-status", "occupation", "native-country")

    val input = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .csv(inputPath)

    /*
     * Step 1: Pre-processing
     */

    // Step 1.0: Store sensitive attributes
    val sensitiveAttributes: List[String] = input.select(sensitiveAttributeColumn).distinct().collect().map(_.getString(0)).toList

    // Step 1.1: All non quasi-identifier attributes are removed
    val subset = input.select(QIDs.head, QIDs.tail: _*) //Pass each element of QID as its own argument to select

    println("Dataset with QIDs selected")
    subset.show()

    // Step 1.2: Tuples with the same quasi-identifier values are grouped together
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    println("Dataset grouped by QIDs")
    subsetWithK.show()

    /*
     * Step 2: Generalization
     */

    val taxonomyTreeSource = Source.fromFile(taxonomyTreePath)
    val taxonomyTreeString = try taxonomyTreeSource.mkString finally taxonomyTreeSource.close()
    val anonymizationLevels = taxonomyTreeString.parseOption.get

    // Step 2.1: Calculate scores for education_any taxonomy tree

    spark.stop()
  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

}
