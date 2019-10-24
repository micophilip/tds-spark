import argonaut.Argonaut._
import argonaut._
import org.apache.spark.sql.SparkSession

import scala.math._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum

import scala.annotation.tailrec
import scala.io.Source

object TopDownSpecialization extends Serializable {

  val spark = SparkSession.builder().appName("TopDownSpecialization")
    .config("spark.master", "local").getOrCreate()

  def main(args: Array[String]) = {

    import spark.implicits._

    val inputPath = args(0)
    val taxonomyTreePath = args(1)
    val k = args(2)
    val sensitiveAttributeColumn = args(3)
    val countColumn = "count"

    println(s"Anonymizing dataset in $inputPath")
    println(s"Running TDS with k = $k")

    val QIDs = List("age", "education", "marital-status", "occupation", "native-country", sensitiveAttributeColumn)

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

    val fieldToScore = "education"

    val generalizedValue = anonymizationLevels.field(fieldToScore).get.field("parent").get.stringOrEmpty

    val generalizedField = s"${fieldToScore}_parent"

    val subsetAnyEdu = subsetWithK.withColumn(generalizedField, lit(generalizedValue))

    subsetAnyEdu.show()

    val firstSANumerator = subsetAnyEdu.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(0)}' and $generalizedField = '$generalizedValue'").agg(sum(countColumn)).first.getLong(0)
    val secondSANumerator = subsetAnyEdu.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(1)}' and $generalizedField = '$generalizedValue'").agg(sum(countColumn)).first.getLong(0)

    val denominator = subsetAnyEdu.where(s"$generalizedField = '$generalizedValue'").agg(sum(countColumn)).first.getLong(0)

    println(s"First Numerator: $firstSANumerator")
    println(s"Second Numerator: $secondSANumerator")
    println(s"Denominator: $denominator")

    val firstSADivision = firstSANumerator.toDouble / denominator.toDouble
    val firstEntropy = (-firstSADivision) * log2(firstSADivision)

    println(s"First Entropy: $firstEntropy")

    val secondSADivision = secondSANumerator.toDouble / denominator.toDouble
    val secondEntropy = (-secondSADivision) * log2(secondSADivision)

    println(s"Second Entropy: $secondEntropy")

    val entropy = firstEntropy + secondEntropy

    println(s"Entropy: $entropy")

    spark.stop()
  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

  //TODO: tailrec, more efficient?
  def findAncestor(tree: Json, node: String): String = {
    val parent = tree.field("parent").get.stringOrEmpty
    val leaves = tree.field("leaves").get.arrayOrEmpty

    leaves.foreach(j => {
      if (node != j.field("parent").get.stringOrEmpty) {
        findAncestor(leaves.head, node)
      }
    })

    parent
  }

}
