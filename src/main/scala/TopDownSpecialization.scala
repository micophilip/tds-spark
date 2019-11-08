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
import org.apache.spark.sql.functions.min

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.io.Source

case class TopScoringAL(QID: String, parent: String)

object TopDownSpecialization extends Serializable {

  // TODO: Fix linting issues

  val spark: SparkSession = SparkSession.builder().appName("TopDownSpecialization")
    .config("spark.master", "local").getOrCreate()

  val GENERALIZED_POSTFIX = "_generalized"

  def main(args: Array[String]) = {

    import spark.implicits._

    val inputPath = args(0)
    val taxonomyTreePath = args(1)
    val k = args(2).toInt
    val sensitiveAttributeColumn = args(3)

    println(s"Anonymizing dataset in $inputPath")
    println(s"Running TDS with k = $k")

    val QIDsOnly = List("education", "marital_status", "occupation", "native_country")
    val QIDsGeneralized = QIDsOnly.map(_ + GENERALIZED_POSTFIX)

    val QIDsUnionSensitiveAttributes = QIDsOnly ::: List(sensitiveAttributeColumn)

    val input = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .csv(inputPath)

    /*
     * Step 1: Pre-processing
     */

    // Step 1.0: Store sensitive attributes
    //TODO: Define globally along with sensitiveAttributeColumn
    val sensitiveAttributes: List[String] = input.select(sensitiveAttributeColumn).distinct().collect().map(_.getString(0)).toList

    // Step 1.1: All non quasi-identifier attributes are removed
    val subset = input.select(QIDsUnionSensitiveAttributes.head, QIDsUnionSensitiveAttributes.tail: _*) //Pass each element of QID as its own argument to select

    // Step 1.2: Tuples with the same quasi-identifier values are grouped together
    val subsetWithK = subset.groupBy(QIDsUnionSensitiveAttributes.head, QIDsUnionSensitiveAttributes.tail: _*).count()

    /*
     * Step 2: Generalization
     */

    val taxonomyTreeSource = Source.fromFile(taxonomyTreePath)
    val taxonomyTreeString = try taxonomyTreeSource.mkString finally taxonomyTreeSource.close()
    val taxonomyTree = taxonomyTreeString.parseOption.get
    val anonymizationLevels = QIDsOnly.map(QID => {
      val anonymizationLevelTree = taxonomyTree.field(QID).get
      Json("field" -> jString(QID), "tree" -> anonymizationLevelTree)
    })

    val fullPathMap: Map[String, Map[String, Queue[String]]] = Map[String, Map[String, Queue[String]]]()

    QIDsOnly.foreach(QID => {
      fullPathMap += (QID -> buildPathMapFromTree(taxonomyTree.field(QID).get))
    })

    // Step 2.1: Calculate scores for each QID in taxonomy tree
    /*
     * For all attributes, generalize from leaf to root
     * Calculate kCurrent
     * If kCurrent > k, too generalized, find highest score for all anonymization levels (AL)
     * Remove root of top scoring AL and add its children to ALs
     */

    subsetWithK.cache()

    val generalizedDF = generalize(fullPathMap, subsetWithK, QIDsOnly, 0)
    val kCurrent = calculateK(generalizedDF, QIDsGeneralized)

    println(s"Initial K is $kCurrent")

    if (kCurrent > k) {
      anonymize(fullPathMap, QIDsOnly, anonymizationLevels, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes, k)
    } else {
      println(s"Dataset is $kCurrent-anonymous and required is $k")
    }

    subsetWithK.unpersist()

    spark.stop()

  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

  def deepCopy(source: Map[String, Map[String, Queue[String]]]): Map[String, Map[String, Queue[String]]] = {
    source.map(t => {
      t._1 -> t._2.map(l => {
        l._1 -> l._2.clone
      })
    })
  }

  def getRoot(tree: Json): String = {
    tree.field("parent").get.stringOrEmpty
  }

  def getChildren(tree: Json): JsonArray = {
    tree.field("leaves").get.arrayOrEmpty
  }

  def updatePathMap(pathMap: Map[String, Map[String, Queue[String]]], field: String, parent: String): Map[String, Map[String, Queue[String]]] = {
    pathMap(field).keys.foreach(key => {
      val queue = pathMap(field)(key)
      if (queue.nonEmpty && queue.head == parent && queue.size > 1) queue.dequeue()
    })
    pathMap
  }

  def goToNextLevel(anonymizationLevels: JsonArray, maxScoreColumn: String, maxScoreParent: String): JsonArray = {
    val maxScoreTree = anonymizationLevels.find(j => {
      j.field("field").get.stringOrEmpty == maxScoreColumn && getRoot(j.field("tree").get) == maxScoreParent
    })
    val maxScoreChildren = if (maxScoreTree.isEmpty || maxScoreTree.get.field("tree").isEmpty) List() else getChildren(maxScoreTree.get.field("tree").get).map(c => {
      Json("field" -> jString(maxScoreColumn), "tree" -> c)
    })

    anonymizationLevels.filter(j => {
      j.field("field").get.stringOrEmpty != maxScoreColumn || getRoot(j.field("tree").get) != maxScoreParent
    }) ::: maxScoreChildren
  }

  @tailrec
  def generalize(fullPathMap: Map[String, Map[String, Queue[String]]], input: DataFrame, QIDs: List[String], level: Int): DataFrame = {

    QIDs match {
      case Nil => input
      case head :: tail =>
        val output = input.withColumn(head + GENERALIZED_POSTFIX, findAncestorUdf(fullPathMap(head), level)(input(head)))
        generalize(fullPathMap, output, tail, level)
    }
  }

  def anonymize(fullPathMap: Map[String, Map[String, Queue[String]]], QIDsOnly: List[String], anonymizationLevels: JsonArray, subsetWithK: DataFrame, sensitiveAttributeColumn: String, sensitiveAttributes: List[String], requestedK: Int): Map[String, Map[String, Queue[String]]] = {

    val originalMap = Map[String, Map[String, Queue[String]]]()
    val QIDsGeneralized = QIDsOnly.map(_ + GENERALIZED_POSTFIX)
    subsetWithK.cache()

    @tailrec
    def anonymizeOneLevel(originalPathMap: Map[String, Map[String, Queue[String]]], fullPathMap: Map[String, Map[String, Queue[String]]], anonymizationLevels: JsonArray): Map[String, Map[String, Queue[String]]] = {
      val updatedScores = calculateScores(fullPathMap, QIDsOnly, anonymizationLevels, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes, Map[Double, TopScoringAL]())
      val maxScore = updatedScores.keys.toList.max
      val maxScoreAL = updatedScores(maxScore)
      val maxScoreColumn = maxScoreAL.QID
      val maxScoreParent = maxScoreAL.parent
      val anonymous = generalize(fullPathMap, subsetWithK, List(maxScoreColumn), 0)
      val kCurrent = calculateK(anonymous, QIDsGeneralized)
      if (kCurrent > requestedK) {
        val newALs = goToNextLevel(anonymizationLevels, maxScoreColumn, maxScoreParent)
        val previousMap = deepCopy(fullPathMap)
        val updatedMap = updatePathMap(fullPathMap, maxScoreColumn, maxScoreParent)
        anonymizeOneLevel(previousMap, updatedMap, newALs)
      } else if (kCurrent < requestedK) {
        originalPathMap
      } else {
        fullPathMap
      }
    }

    val anonymized = anonymizeOneLevel(originalMap, fullPathMap, anonymizationLevels)

    subsetWithK.unpersist()

    anonymized
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

  @tailrec
  def calculateScores(fullPathMap: Map[String, Map[String, Queue[String]]], QIDs: List[String], anonymizationLevels: JsonArray, subsetWithK: DataFrame, sensitiveAttributeColumn: String, sensitiveAttributes: List[String], scores: Map[Double, TopScoringAL]): Map[Double, TopScoringAL] = {

    anonymizationLevels match {
      case Nil => scores
      case head :: tail =>
        val QID = head.field("field").get.stringOrEmpty
        val tree = head.field("tree").get
        val parent = tree.field("parent").get.stringOrEmpty
        val score = calculateScore(fullPathMap(QID), tree, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes, QID)
        scores += (score -> TopScoringAL(QID, parent))
        calculateScores(fullPathMap, QIDs, tail, subsetWithK, sensitiveAttributeColumn, sensitiveAttributes, scores)
    }

  }

  def calculateK(dataset: DataFrame, columns: List[String]): Long = {
    dataset.na.drop().groupBy(columns.head, columns.tail: _*).agg(sum("count").alias("sum")).agg(min("sum")).first.getLong(0)
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
          //TODO: Look for a shortcut for this
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
      val trimmedNode = node.trim
      if (!fullPathMap.contains(trimmedNode) || fullPathMap(trimmedNode).isEmpty)
        None
      else if (fullPathMap(trimmedNode).size <= level)
        Some(fullPathMap(trimmedNode).head)
      else
        Some(fullPathMap(trimmedNode)(level))
    }

  }

  def findAncestorUdf(fullPathMap: Map[String, Queue[String]], level: Int): UserDefinedFunction = udf((value: String) => {
    findAncestor(fullPathMap, value, level)
  })

  def calculateScore(fullPathMap: Map[String, Queue[String]], tree: Json, subsetWithK: DataFrame, sensitiveAttributeColumn: String, sensitiveAttributes: List[String], fieldToScore: String): Double = {

    val countColumn = "count" //TODO: Move to global scope

    val generalizedField = s"$fieldToScore$GENERALIZED_POSTFIX"
    val generalizedValue = tree.field("parent").get.stringOrEmpty

    val children = tree.field("leaves").get.arrayOrEmpty

    val subsetAnyEdu = subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 0)(subsetWithK(fieldToScore)))

    val denominatorDf = subsetAnyEdu.where(s"$generalizedField = '$generalizedValue'").agg(sum(countColumn)).first

    if (denominatorDf.get(0) == null) {
      0
    } else {
      subsetWithK.cache()
      subsetAnyEdu.cache()
      val denominator = denominatorDf.getLong(0)
      val entropy = sensitiveAttributes.map(sensitiveAttribute => {
        val numerator = subsetAnyEdu.where(s"$sensitiveAttributeColumn = '${sensitiveAttribute}' and $generalizedField = '$generalizedValue'").agg(sum(countColumn)).first
        if (numerator.get(0) != null) {
          val division = numerator.getLong(0).toDouble / denominator.toDouble
          (-division) * log2(division)
        } else {
          0
        }
      }).sum

      val childEntropyDf = subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 1)(subsetWithK(fieldToScore)))

      val denominatorMap: Map[String, Long] = Map[String, Long]()
      val childDenominatorList: ListBuffer[Long] = ListBuffer[Long]()

      //TODO: Make recursive
      children.foreach(child => {
        val first = childEntropyDf.where(s"$generalizedField = '${getRoot(child)}'").agg(sum(countColumn)).first()
        if (first.get(0) != null) {
          val childDenominator = first.getLong(0)
          childDenominatorList += childDenominator
          denominatorMap += (getRoot(child) -> childDenominator)
        }
      })

      val entropyMap: Map[String, Double] = Map[String, Double]()

      //TODO: Make recursive
      children.foreach(child => {
        sensitiveAttributes.foreach(sensitiveAttribute => {
          val first = childEntropyDf.where(s"$sensitiveAttributeColumn = '${sensitiveAttribute}' and $generalizedField = '${getRoot(child)}'").agg(sum(countColumn)).first
          if (denominatorMap.contains(getRoot(child)) && first.get(0) != null) {
            val numerator = first.getLong(0)
            val division = numerator.toDouble / denominatorMap(getRoot(child))
            val entropy = (-division) * log2(division)
            if (entropyMap.get(getRoot(child)).isEmpty) entropyMap += (getRoot(child) -> entropy)
            else entropyMap += (getRoot(child) -> (entropyMap(getRoot(child)) + entropy))
          }
        })
      })

      val childrenEntropy = children.map(child => {
        val node = getRoot(child)
        if (denominatorMap.contains(node)) {
          val childDenominatorFromMap = denominatorMap(node)
          val childEntropy = entropyMap(node)
          (childDenominatorFromMap.toDouble / denominator.toDouble) * childEntropy
        } else {
          0
        }
      }).sum

      val infoGain = entropy - childrenEntropy

      val anonymity = denominator
      val anonymityPrime = if (childDenominatorList.isEmpty) 0 else childDenominatorList.min
      val anonymityLoss = (anonymity - anonymityPrime).toDouble

      val score = if (anonymityLoss == 0.0) infoGain else (infoGain.toDouble / (anonymity - anonymityPrime).toDouble)

      println(s"Info gain is $infoGain")
      println(s"anonymity is $anonymity")
      println(s"anonymityPrime is $anonymityPrime")
      println(s"Score for ${fieldToScore}_$generalizedValue is $score")

      subsetAnyEdu.unpersist()
      subsetWithK.unpersist()

      score

    }

  }

}
