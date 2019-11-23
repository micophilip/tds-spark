import argonaut.Argonaut._
import argonaut._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, min, monotonically_increasing_id, sum, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math._

case class TopScoringAL(QID: String, parent: String)

object TDSConstants {

  val spark: SparkSession = SparkSession.builder().appName("TopDownSpecialization").getOrCreate()

  val GENERALIZED_POSTFIX = "_generalized"

  val sensitiveAttributes: List[String] = List("<=50K", ">50K")
  val sensitiveAttributeColumn = "income"

  val countColumn = "count"
}

import TDSConstants._

object TopDownSpecialization extends Serializable {

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val k = args(1).toInt
    val parallelism = spark.sparkContext.defaultParallelism

    println(s"Anonymizing dataset in $inputPath")
    println(s"Running TDS with k = $k")

    val QIDsOnly = List("education", "marital_status", "occupation", "native_country", "workclass", "relationship", "race", "sex")
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
      .withColumn("rowId", monotonically_increasing_id).repartition(col("rowId"))

    /*
     * Step 2: Generalization
     */

    val taxonomyTreeSource = Source.fromResource("taxonomytree.json")
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

    val generalizedDF = generalize(fullPathMap, subsetWithK, QIDsOnly, 0).persist(StorageLevel.MEMORY_ONLY)
    val kCurrent = calculateK(generalizedDF, QIDsGeneralized)

    println(s"Initial K is $kCurrent")
    val start = System.currentTimeMillis()

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
    println(s"Parallelism used was $parallelism")

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

    val QIDsGeneralized = QIDsOnly.map(_ + GENERALIZED_POSTFIX)

    @tailrec
    def anonymizeOneLevel(originalPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], anonymizationLevels: JsonArray): mutable.Map[String, mutable.Map[String, mutable.Queue[String]]] = {
      val df = prepareAggregationsDF(fullPathMap, QIDsOnly, anonymizationLevels, subsetWithK)
      val maxScoreAL = findBestAL(df, QIDsOnly, anonymizationLevels)
      val maxScoreColumn = maxScoreAL.QID
      val maxScoreParent = maxScoreAL.parent
      val newALs = goToNextLevel(anonymizationLevels, maxScoreColumn, maxScoreParent)
      val previousMap = deepCopy(fullPathMap)
      val updatedMap = updatePathMap(fullPathMap, maxScoreColumn, maxScoreParent)
      val anonymous = generalize(updatedMap, subsetWithK, QIDsOnly, 0)
      val kCurrent = calculateK(anonymous, QIDsGeneralized)
      if (kCurrent > requestedK) {
        anonymizeOneLevel(previousMap, updatedMap, newALs)
      } else if (kCurrent < requestedK) {
        originalPathMap
      } else {
        fullPathMap
      }
    }

    val anonymized = anonymizeOneLevel(fullPathMap, fullPathMap, anonymizationLevels)

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

  @tailrec
  def getRVSV(generalizedField: String, generalizedValue: String, rest: List[String])(dataFrame: DataFrame): DataFrame = {
    rest match {
      case Nil => dataFrame
      case head :: tail =>
        getRVSV(generalizedField, generalizedValue, tail)(dataFrame.withColumn(generalizedField + "_RVSV_" + sensitiveAttributes.indexOf(head), when(dataFrame(generalizedField) === generalizedValue and dataFrame(sensitiveAttributeColumn) === head, dataFrame(countColumn)).otherwise(0)))
    }
  }

  @tailrec
  def getRVC(generalizedField: String, children: JsonArray)(dataFrame: DataFrame): DataFrame = {
    children match {
      case Nil => dataFrame
      case head :: tail =>
        getRVC(generalizedField, tail)(dataFrame.withColumn(generalizedField + "_RVC_" + getRoot(head), when(dataFrame(generalizedField) === getRoot(head), dataFrame(countColumn)).otherwise(0)))
    }
  }

  @tailrec
  def getRVCSV_SA(generalizedField: String, dataFrame: DataFrame, children: JsonArray, sensitiveAttribute: String): DataFrame = {

    children match {
      case Nil => dataFrame
      case head :: tail =>
        getRVCSV_SA(generalizedField, dataFrame.withColumn(generalizedField + "_RVCSV_" + getRoot(head) + "_" + sensitiveAttributes.indexOf(sensitiveAttribute), when((dataFrame(generalizedField) === getRoot(head)) and (dataFrame(sensitiveAttributeColumn) === sensitiveAttribute), dataFrame(countColumn)).otherwise(0)), tail, sensitiveAttribute)
    }

  }

  @tailrec
  def getRVCSV(generalizedField: String, children: JsonArray, rest: List[String])(dataFrame: DataFrame): DataFrame = {

    rest match {
      case Nil => dataFrame
      case head :: tail =>
        getRVCSV(generalizedField, children, tail)(getRVCSV_SA(generalizedField, dataFrame, children, head))
    }

  }

  @tailrec
  def sumChildrenEntropy(children: JsonArray, denominatorMap: mutable.Map[String, Long], entropyMap: mutable.Map[String, Double], RV_TOTAL: Long, sumSoFar: Double): Double = {
    children match {
      case Nil => sumSoFar
      case child :: tail =>
        val node = getRoot(child)
        if (denominatorMap.contains(node)) {
          val childDenominatorFromMap = denominatorMap(node)
          val childEntropy = entropyMap(node)
          val current = (childDenominatorFromMap.toDouble / RV_TOTAL.toDouble) * childEntropy
          sumChildrenEntropy(tail, denominatorMap, entropyMap, RV_TOTAL, sumSoFar + current)
        } else {
          sumChildrenEntropy(tail, denominatorMap, entropyMap, RV_TOTAL, sumSoFar)
        }
    }
  }


  def calculateScore(aggregationMap: Map[String, Long], QID: String, parent: String, tree: Json): Double = {
    val RV_TOTAL = aggregationMap(s"sum(${QID}_${parent}_RV)")

    if (RV_TOTAL == 0) {
      0
    } else {

      val children = tree.field("leaves").get.arrayOrEmpty
      val denominatorMap: mutable.Map[String, Long] = mutable.Map[String, Long]()
      val childDenominatorList: ListBuffer[Long] = ListBuffer[Long]()
      val entropyMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
      val generalizedChildField = s"$QID${GENERALIZED_POSTFIX}_CHILD"

      val IRV = sensitiveAttributes.map(sensitiveAttribute => {
        val numerator = aggregationMap(s"sum(${QID}_${parent}_RVSV_${sensitiveAttributes.indexOf(sensitiveAttribute)})")
        if (numerator == 0) {
          0
        } else {
          val division = numerator.toDouble / RV_TOTAL.toDouble
          (-division) * log2(division)
        }
      }).sum

      children.foreach(child => {
        val childDenominator = aggregationMap(s"sum(${generalizedChildField}_RVC_${getRoot(child)})")
        if (childDenominator != 0) {
          childDenominatorList += childDenominator
          denominatorMap += (getRoot(child) -> childDenominator)
        }

        sensitiveAttributes.foreach(sensitiveAttribute => {
          val numerator = aggregationMap(s"sum(${generalizedChildField}_RVCSV_${getRoot(child)}_${sensitiveAttributes.indexOf(sensitiveAttribute)})")
          if (denominatorMap.contains(getRoot(child)) && numerator != 0) {
            val division = numerator.toDouble / denominatorMap(getRoot(child))
            val entropy = (-division) * log2(division)
            if (entropyMap.get(getRoot(child)).isEmpty) entropyMap += (getRoot(child) -> entropy)
            else entropyMap += (getRoot(child) -> (entropyMap(getRoot(child)) + entropy))
          }
        })

      })

      val childrenEntropy = sumChildrenEntropy(children, denominatorMap, entropyMap, RV_TOTAL, 0.0)

      val infoGain = IRV - childrenEntropy

      val anonymity = RV_TOTAL
      val anonymityPrime = if (childDenominatorList.isEmpty) 0 else childDenominatorList.min
      val anonymityLoss = (anonymity - anonymityPrime).toDouble
      if (anonymityLoss == 0.0) infoGain else infoGain.toDouble / anonymityLoss
    }

  }

  @tailrec
  def prepareAggregationsDF(fullPathMap: mutable.Map[String, mutable.Map[String, mutable.Queue[String]]], QIDs: List[String], anonymizationLevels: JsonArray, subsetWithK: DataFrame): DataFrame = {

    anonymizationLevels match {
      case Nil => subsetWithK
      case head :: tail =>
        val QID = head.field("field").get.stringOrEmpty
        val tree = head.field("tree").get
        val parent = tree.field("parent").get.stringOrEmpty
        val generalizedField = s"${QID}_$parent"
        val generalizedChildField = s"$QID${GENERALIZED_POSTFIX}_CHILD"
        val generalizedValue = tree.field("parent").get.stringOrEmpty
        val children = tree.field("leaves").get.arrayOrEmpty
        val df = subsetWithK.transform(transform(generalizedField, generalizedChildField, fullPathMap(QID), QID, generalizedValue, children))
        prepareAggregationsDF(fullPathMap, QIDs, tail, df)
    }

  }

  def transform(generalizedField: String, generalizedChildField: String, fullPathMap: mutable.Map[String, mutable.Queue[String]], fieldToScore: String, generalizedValue: String, children: JsonArray)(subsetWithK: DataFrame): DataFrame = {
    subsetWithK.withColumn(generalizedField, findAncestorUdf(fullPathMap, 0)(subsetWithK(fieldToScore)))
      .withColumn(generalizedChildField, findAncestorUdf(fullPathMap, 1)(subsetWithK(fieldToScore)))
      .withColumn(generalizedField + "_RV", when(col(generalizedField) === generalizedValue, col(countColumn)).otherwise(0))
      .transform(getRVSV(generalizedField, generalizedValue, sensitiveAttributes))
      .transform(getRVC(generalizedChildField, children))
      .transform(getRVCSV(generalizedChildField, children, sensitiveAttributes))
  }

  def getAggregationMap(anonymizationLevels: JsonArray, df: DataFrame): Map[String, Long] = {

    val RVMaps: Map[String, String] = anonymizationLevels.map(AL => {
      val fieldToScore = AL.field("field").get.stringOrEmpty
      val tree = AL.field("tree").get
      val parent = tree.field("parent").get.stringOrEmpty
      s"${fieldToScore}_${parent}_RV" -> "sum"
    }).toMap

    val RVSVMaps: Map[String, String] = anonymizationLevels.flatMap(AL => {
      val fieldToScore = AL.field("field").get.stringOrEmpty
      val tree = AL.field("tree").get
      val parent = tree.field("parent").get.stringOrEmpty
      sensitiveAttributes.map(sensitiveAttribute => {
        s"${fieldToScore}_${parent}_RVSV_${sensitiveAttributes.indexOf(sensitiveAttribute)}" -> "sum"
      }).toMap
    }).toMap

    val RVCMaps: Map[String, String] = anonymizationLevels.flatMap(anonymizationLevel => {

      val fieldToScore = anonymizationLevel.field("field").get.stringOrEmpty
      val tree = anonymizationLevel.field("tree").get
      val generalizedChildField = s"$fieldToScore${GENERALIZED_POSTFIX}_CHILD"
      val children = tree.field("leaves").get.arrayOrEmpty
      children.map(child => {
        s"${generalizedChildField}_RVC_${getRoot(child)}" -> "sum"
      }).toMap

    }).toMap

    val RVCSAMaps: Map[String, String] = anonymizationLevels.flatMap(anonymizationLevel => {
      val fieldToScore = anonymizationLevel.field("field").get.stringOrEmpty
      val tree = anonymizationLevel.field("tree").get
      val generalizedChildField = s"$fieldToScore${GENERALIZED_POSTFIX}_CHILD"
      val children = tree.field("leaves").get.arrayOrEmpty
      children.flatMap(child => {
        sensitiveAttributes.map(sensitiveAttribute => {
          s"${generalizedChildField}_RVCSV_${getRoot(child)}_${sensitiveAttributes.indexOf(sensitiveAttribute)}" -> "sum"
        }).toMap
      }).toMap

    }).toMap

    val aggregations = RVMaps ++ RVSVMaps ++ RVCMaps ++ RVCSAMaps

    val aggregationMapDf = df.agg(aggregations)

    val keys = aggregationMapDf.columns
    val values = aggregationMapDf.toLocalIterator().next()

    keys.zipWithIndex.map(key => {
      key._1 -> values.getLong(key._2)
    }).toMap

  }

  def findBestAL(df: DataFrame, QIDs: List[String], anonymizationLevels: JsonArray): TopScoringAL = {

    val scores: mutable.Map[Double, TopScoringAL] = mutable.Map[Double, TopScoringAL]()

    val aggregationMap = getAggregationMap(anonymizationLevels, df)

    anonymizationLevels.foreach(AL => {

      val QID = AL.field("field").get.stringOrEmpty
      val tree = AL.field("tree").get
      val parent = tree.field("parent").get.stringOrEmpty

      scores += (calculateScore(aggregationMap, QID, parent, tree) -> TopScoringAL(QID, parent))

    })

    scores(scores.keys.toList.max)

  }

}
