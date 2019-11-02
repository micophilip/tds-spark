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
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
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

    val pathMap = buildPathMapFromTree(anonymizationLevels)


    // Step 2.1: Calculate scores for education_any taxonomy tree
    //    calculateScore(anonymizationLevels, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes)

    spark.stop()
  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

  def getRoot(tree: Json): String = {
    tree.field("parent").get.stringOrEmpty
  }

  def getChildren(tree: Json): JsonArray = {
    tree.field("leaves").get.arrayOrEmpty
  }

  @tailrec
  def getPath(pathMap: Map[String, String], node: String, currentPath: Queue[String]): Queue[String] = {
    val parent = pathMap.get(node)
    if (parent.isEmpty) currentPath += node
    else getPath(pathMap, parent.get, currentPath += node)
  }

  def isLeaf(tree: Json): Boolean = {
    getChildren(tree).isEmpty
  }

  // Breadth-first search
  @tailrec
  def getParentChildMapping(children: JsonArray, parentQueue: Queue[String], pathMap: Map[String, String]): Map[String, String] = {

    children match {
      case Nil => pathMap
      case head :: tail => {
        val parent = parentQueue.dequeue()
        pathMap += (getRoot(head) -> parent)
        if (isLeaf(head)) {
          getParentChildMapping(tail, parentQueue, pathMap)
        } else {
          getChildren(head).foreach(child => {
            parentQueue += getRoot(head)
          })
          getParentChildMapping(tail ::: getChildren(head), parentQueue, pathMap)
        }
      }
    }

  }

  def buildPathMapFromTree(tree: Json): Map[String, Queue[String]] = {
    val starterParentQueue: Queue[String] = Queue[String]()

    getChildren(tree).foreach(child => {
      starterParentQueue += getRoot(tree)
    })

    val parentChildMapping = getParentChildMapping(getChildren(tree), starterParentQueue, Map[String, String]())

    val fullPathMap = Map[String, Queue[String]]()

    parentChildMapping.keys.foreach(key => {
      val path = getPath(parentChildMapping, key, Queue[String]())
      fullPathMap += (key -> path.reverse)
    })

    fullPathMap
  }

  def findAncestor(fullPathMap: Map[String, Queue[String]], node: String, level: Int): Option[String] = {

    if (node == null) None
    else {
      if (fullPathMap.get(node.trim).nonEmpty) Some(fullPathMap(node.trim)(level))
      else None
    }

  }

  def findAncestorUdf(fullPathMap: Map[String, Queue[String]], level: Int): UserDefinedFunction = udf((value: String) => {
    findAncestor(fullPathMap, value, level)
  })

  def calculateScore(fullPathMap: Map[String, Queue[String]], anonymizationLevels: Json, subsetWithK: DataFrame, sensitiveAttributeColumn: String, sensitiveAttributes: List[String]): Double = {

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

    val denominator = subsetAnyEdu.where(s"$generalizedField = '$generalizedValue'").agg(sum(countColumn)).first.getLong(0)

    val entropy = sensitiveAttributes.map(sensitiveAttribute => {
      val numerator = subsetAnyEdu.where(s"$sensitiveAttributeColumn = '${sensitiveAttribute}' and $generalizedField = '$generalizedValue'").agg(sum(countColumn)).first.getLong(0)
      val division = numerator.toDouble / denominator.toDouble
      (-division) * log2(division)
    }).sum

    println(s"Entropy: $entropy")

    val childEntropyDf = subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 1)(subsetWithK(fieldToScore)))

    val firstChildFirstSANumerator = childEntropyDf.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(0)}' and $generalizedField = '$firstChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)
    val firstChildSecondSANumerator = childEntropyDf.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(1)}' and $generalizedField = '$firstChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)

    val secondChildFirstSANumerator = childEntropyDf.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(0)}' and $generalizedField = '$secondChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)
    val secondChildSecondSANumerator = childEntropyDf.where(s"$sensitiveAttributeColumn = '${sensitiveAttributes(1)}' and $generalizedField = '$secondChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)

    val firstDenominator = childEntropyDf.where(s"$generalizedField = '$firstChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)
    val secondDenominator = childEntropyDf.where(s"$generalizedField = '$secondChildGeneralizedValue'").agg(sum(countColumn)).first.getLong(0)

    val firstChildFirstSADivision = firstChildFirstSANumerator.toDouble / firstDenominator.toDouble
    val firstChildSecondSADivision = firstChildSecondSANumerator.toDouble / firstDenominator.toDouble

    val secondChildFirstSADivision = secondChildFirstSANumerator.toDouble / secondDenominator.toDouble
    val secondChildSecondSADivision = secondChildSecondSANumerator.toDouble / secondDenominator.toDouble

    val firstChildFirstSAEntropy = (-firstChildFirstSADivision) * log2(firstChildFirstSADivision)
    val firstChildSecondSAEntropy = (-firstChildSecondSADivision) * log2(firstChildSecondSADivision)

    val secondChildFirstSAEntropy = (-secondChildFirstSADivision) * log2(secondChildFirstSADivision)
    val secondChildSecondSAEntropy = (-secondChildSecondSADivision) * log2(secondChildSecondSADivision)

    val firstChildEntropy = firstChildFirstSAEntropy + firstChildSecondSAEntropy
    val secondChildEntropy = secondChildFirstSAEntropy + secondChildSecondSAEntropy

    println(s"First child first numerator is $firstChildFirstSANumerator")
    println(s"First child second numerator is $firstChildSecondSANumerator")
    println(s"First child denominator is $firstDenominator")

    println(s"Second child first numerator is $secondChildFirstSANumerator")
    println(s"Second child second numerator is $secondChildSecondSANumerator")
    println(s"Second child denominator is $secondDenominator")

    println(s"First child entropy is $firstChildEntropy")
    println(s"Second child entropy is $secondChildEntropy")

    val infoGain = entropy - (((firstDenominator.toDouble / denominator.toDouble) * firstChildEntropy) + ((secondDenominator.toDouble / denominator.toDouble) * secondChildEntropy))

    val anonymity = denominator
    val anonymityPrime = min(firstDenominator, secondDenominator)

    val score = infoGain.toDouble / (anonymity - anonymityPrime).toDouble

    println(s"Info gain is $infoGain")
    println(s"anonymity is $anonymity")
    println(s"anonymityPrime is $anonymityPrime")
    println(s"Score is $score")

    score

  }

}
