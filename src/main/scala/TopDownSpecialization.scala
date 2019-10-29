import argonaut.Argonaut._
import argonaut._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.udf

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
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
    calculateEntropy(anonymizationLevels, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes)

    spark.stop()
  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

  def findAncestor(tree: Json, node: String): Option[String] = {

    if (node == null) None
    else {
      val serialized: ListBuffer[String] = ListBuffer.empty[String]

      @tailrec
      def visit(subTree: Json, children: JsonArray, node: String): Unit = {
        children match {
          case Nil =>
          case x :: tail => if (x.field("parent").get.stringOrEmpty == node.trim) serialized += node else visit(x, tail ::: x.field("leaves").get.arrayOrEmpty, node)
        }
      }

      visit(tree, tree.field("leaves").get.arrayOrEmpty, node)

      if (serialized.nonEmpty) Some(tree.field("parent").get.stringOrEmpty)
      else None
    }

  }

  def findAncestorUdf(tree: Json): UserDefinedFunction = udf((value: String) => {
    findAncestor(tree, value)
  })

  def calculateEntropy(anonymizationLevels: Json, subsetWithK: DataFrame, sensitiveAttributeColumn: String, sensitiveAttributes: List[String]): Double = {

    val countColumn = "count"
    val fieldToScore = "education"

    val generalizedField = s"${fieldToScore}_parent"
    val generalizedValue = anonymizationLevels.field(fieldToScore).get.field("parent").get.stringOrEmpty

    val children = anonymizationLevels.field(fieldToScore).get.field("leaves").get.arrayOrEmpty

    // For every child - for now, we know we have two children!

    val firstChild = children(0)
    val secondChild = children(1)

    val firstChildGeneralizedValue = firstChild.field("parent").get.stringOrEmpty
    val secondChildGeneralizedValue = secondChild.field("parent").get.stringOrEmpty

    // Rerun calculation for every child, withColumn call should be with generalized value (root of tree that the leaf belongs to)

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

    val firstChildEntropyDf = subsetWithK.withColumn(generalizedField, findAncestorUdf(secondChild)(subsetWithK(fieldToScore)))

    //TODO: All fields are null due to the recursive call inside the UDF - not serializable? serialized.nonEmpty is always false, serialize tree first then call contains?
    firstChildEntropyDf.show()

    entropy

  }

}
