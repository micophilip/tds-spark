import argonaut.Argonaut._
import argonaut._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{min, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math._

case class TopScoringAL(QID: String, parent: String)

object TopDownSpecialization extends Serializable {

  val spark: SparkSession = SparkSession.builder().appName("TopDownSpecialization")
    .config("spark.master", "local[8]").getOrCreate()

  val GENERALIZED_POSTFIX = "_generalized"

  val sensitiveAttributes: List[String] = List("<=50", ">50")
  val sensitiveAttributeColumn = "income"

  val countColumn = "count"

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val taxonomyTreePath = args(1)
    val k = args(2).toInt
    val start = System.currentTimeMillis()

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

    // Step 1.1: All non quasi-identifier attributes are removed
    val subset = input.select(QIDsUnionSensitiveAttributes.head, QIDsUnionSensitiveAttributes.tail: _*)

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

    val fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = mutable.Map[String, mutable.Map[String, mutable.Queue[String]]]()

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

    val generalizedDF = generalize(fullPathMap, subsetWithK, QIDsOnly, 0).repartition(8).cache()
    val kCurrent = calculateK(generalizedDF, QIDsGeneralized)

    println(s"Initial K is $kCurrent")

    if (kCurrent > k) {
      val anonymizedLevels = anonymize(fullPathMap, QIDsOnly, anonymizationLevels, generalizedDF, k)
      val anonymizedDataset = generalize(anonymizedLevels, subsetWithK, QIDsOnly, 0)
      val kFinal = calculateK(anonymizedDataset, QIDsGeneralized)
      println(s"Successfully anonymized dataset to k=$kFinal")
    } else {
      println(s"Dataset is $kCurrent-anonymous and required is $k. No further anonymization necessary")
    }

    val finish = System.currentTimeMillis()

    println(s"Anonymized dataset in ${finish - start} ms")

    generalizedDF.unpersist()

    spark.stop()

  }

  def log2(value: Double): Double = {
    log(value) / log(2.0)
  }

  def deepCopy(source: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]]): mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = {
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

  def updatePathMap(pathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], field: String, parent: String): mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = {
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
  def generalize(fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], input: DataFrame, QIDs: List[String], level: Int): DataFrame = {

    QIDs match {
      case Nil => input
      case head :: tail =>
        val output = input.withColumn(head + GENERALIZED_POSTFIX, findAncestorUdf(fullPathMap(head), level)(input(head)))
        generalize(fullPathMap, output, tail, level)
    }
  }

  def anonymize(fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], QIDsOnly: List[String], anonymizationLevels: JsonArray, subsetWithK: DataFrame, requestedK: Int): mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = {

    val originalMap = mutable.Map[String, mutable.Map[String, mutable.Queue[String]]]()
    val QIDsGeneralized = QIDsOnly.map(_ + GENERALIZED_POSTFIX)
    subsetWithK.cache()

    @tailrec
    def anonymizeOneLevel(originalPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], anonymizationLevels: JsonArray): mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = {
      val updatedScores = calculateScores(fullPathMap, QIDsOnly, anonymizationLevels, subsetWithK, mutable.Map[Double, TopScoringAL]())
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
  def getPath(pathMap: mutable.Map[String, String], node: String, currentPath: mutable.Queue[String]): mutable.Queue[String] = {
    val parent = pathMap.get(node)
    if (parent.isEmpty) currentPath += node
    else getPath(pathMap, parent.get, currentPath += node)
  }

  def isLeaf(tree: Json): Boolean = {
    getChildren(tree).isEmpty
  }

  @tailrec
  def calculateScores(fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], QIDs: List[String], anonymizationLevels: JsonArray, subsetWithK: DataFrame, scores: mutable.Map[Double, TopScoringAL]): mutable.Map[Double, TopScoringAL] = {

    anonymizationLevels match {
      case Nil => scores
      case head :: tail =>
        val QID = head.field("field").get.stringOrEmpty
        val tree = head.field("tree").get
        val parent = tree.field("parent").get.stringOrEmpty
        val score = calculateScore(fullPathMap(QID), tree, subsetWithK, QID)
        scores += (score -> TopScoringAL(QID, parent))
        calculateScores(fullPathMap, QIDs, tail, subsetWithK, scores)
    }

  }

  def calculateK(dataset: DataFrame, columns: List[String]): Long = {
    dataset.na.drop().groupBy(columns.head, columns.tail: _*).agg(sum("count").alias("sum")).agg(min("sum")).first.getLong(0)
  }

  // Breadth-first search
  @tailrec
  def getParentChildMapping(children: JsonArray, parentQueue: mutable.Queue[String], pathMap: mutable.Map[String, String]): mutable.Map[String, String] = {

    children match {
      case Nil => pathMap
      case head :: tail =>
        val parent = parentQueue.dequeue()
        pathMap += (getRoot(head) -> parent)
        if (isLeaf(head)) {
          getParentChildMapping(tail, parentQueue, pathMap)
        } else {
          getChildren(head).foreach(_ => {
            parentQueue += getRoot(head)
          })
          getParentChildMapping(tail ::: getChildren(head), parentQueue, pathMap)
        }
    }

  }

  def buildPathMapFromTree(tree: Json): mutable.Map[String, mutable.Queue[String]] = {
    val starterParentQueue: mutable.Queue[String] = mutable.Queue[String]()

    getChildren(tree).foreach(_ => {
      starterParentQueue += getRoot(tree)
    })

    val parentChildMapping = getParentChildMapping(getChildren(tree), starterParentQueue, mutable.Map[String, String]())

    val fullPathMap = mutable.Map[String, mutable.Queue[String]]()

    parentChildMapping.keys.foreach(key => {
      val path = getPath(parentChildMapping, key, mutable.Queue[String]())
      fullPathMap += (key -> path.reverse)
    })

    fullPathMap
  }

  def findAncestor(fullPathMap: mutable.Map[String, mutable.Queue[String]], node: String, level: Int): Option[String] = {

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

  def findAncestorUdf(fullPathMap: mutable.Map[String, mutable.Queue[String]], level: Int): UserDefinedFunction = udf((value: String) => {
    findAncestor(fullPathMap, value, level)
  })

  def calculateScore(fullPathMap: mutable.Map[String, mutable.Queue[String]], tree: Json, subsetWithK: DataFrame, fieldToScore: String): Double = {

    val generalizedField = s"$fieldToScore$GENERALIZED_POSTFIX"
    val generalizedValue = tree.field("parent").get.stringOrEmpty

    val children = tree.field("leaves").get.arrayOrEmpty
    val denominatorMap: mutable.Map[String, Long] = mutable.Map[String, Long]()
    val childDenominatorList: ListBuffer[Long] = ListBuffer[Long]()
    val entropyMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val subsetAnyEdu = subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 0)(subsetWithK(fieldToScore)))

    val denominatorDf = subsetAnyEdu.where(s"$generalizedField = '$generalizedValue'").agg(sum(countColumn)).first

    if (denominatorDf.get(0) == null) {
      0
    } else {
      subsetWithK.cache()
      subsetAnyEdu.cache()
      val denominator = denominatorDf.getLong(0)
      val entropy = sensitiveAttributes.map(sensitiveAttribute => {
        val numerator = subsetAnyEdu.where(s"$sensitiveAttributeColumn = '$sensitiveAttribute' and $generalizedField = '$generalizedValue'").agg(sum(countColumn)).first
        if (numerator.get(0) != null) {
          val division = numerator.getLong(0).toDouble / denominator.toDouble
          (-division) * log2(division)
        } else {
          0
        }
      }).sum

      val childEntropyDf = subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 1)(subsetWithK(fieldToScore)))

      children.foreach(child => {
        val first = childEntropyDf.where(s"$generalizedField = '${getRoot(child)}'").agg(sum(countColumn)).first()
        if (first.get(0) != null) {
          val childDenominator = first.getLong(0)
          childDenominatorList += childDenominator
          denominatorMap += (getRoot(child) -> childDenominator)
        }
      })

      children.foreach(child => {
        sensitiveAttributes.foreach(sensitiveAttribute => {
          val first = childEntropyDf.where(s"$sensitiveAttributeColumn = '$sensitiveAttribute' and $generalizedField = '${getRoot(child)}'").agg(sum(countColumn)).first
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

      val score = if (anonymityLoss == 0.0) infoGain else infoGain.toDouble / anonymityLoss

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
