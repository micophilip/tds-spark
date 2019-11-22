import argonaut.Argonaut._
import argonaut._
import org.scalatest._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

class TopDownSpecializationTest extends FunSuite with BeforeAndAfterAll with Matchers {

  val resource: BufferedSource = Source.fromResource("taxonomytree.json")
  val taxonomyTree: String = try resource.mkString finally resource.close()
  val taxonomyTreeJson: Json = taxonomyTree.parseOption.get

  val spark: SparkSession = SparkSession.builder().appName("TopDownSpecializationTest")
    .config("spark.master", "local[*]").getOrCreate()

  val testData: String = getClass.getResource("/test_data.csv").getPath

  val input: DataFrame = spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .csv(testData)

  override def afterAll {
    spark.stop()
  }

  test("findAncestor should return correct ancestor") {

    val educationTaxonomyTree = taxonomyTreeJson.field("education").get

    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree), "5th-6th", 0) should contain("Any")
    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "10th", 0) should contain("Without-Post-Secondary")
    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "Preschool", 0) should contain("Without-Post-Secondary")

    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head), "Assoc-voc ", 0) should contain("Post-secondary")

    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "University", 0) shouldBe empty
    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head), "Elementary", 0) shouldBe empty

    TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), null, 0) shouldBe empty
  }

  test("calculateScore should accurately calculate score") {

    val QIDs = List("education", "sex", "work_hrs", "income")
    val field = "education"
    val subset = input.select(QIDs.head, QIDs.tail: _*)
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()
    val educationTree = taxonomyTreeJson.field("education").get
    val fullPathMap = mutable.Map[String, mutable.Map[String, mutable.Queue[String]]](field -> TopDownSpecialization.buildPathMapFromTree(educationTree))
    val anonymizationLevels: JsonArray = Json.array(Json("field" -> jString(field), "tree" -> educationTree)).arrayOrEmpty
    val df = TopDownSpecialization.prepareAggregationsDF(fullPathMap, QIDs, anonymizationLevels, subsetWithK)
    val aggregationMap = TopDownSpecialization.getAggregationMap(anonymizationLevels, df)
    val score = TopDownSpecialization.calculateScore(aggregationMap, field, "Any", educationTree)
    score should be(0.0151 +- 0.001)

  }

  test("calculateK should accurately calculate k") {

    val QIDs = List("sex", "work_hrs")

    val subset = input.select(QIDs.head, QIDs.tail: _*)
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    TopDownSpecialization.calculateK(subsetWithK, QIDs) should be(3)
  }

  test("anonymize should anonymize") {
    val field = "education"
    val QIDs = List(field)
    val QIDsGeneralized = QIDs.map(_ + TDSConstants.GENERALIZED_POSTFIX)
    val QIDsUnionSA = QIDs ::: List("income")
    val subset = input.select(QIDsUnionSA.head, QIDsUnionSA.tail: _*)
    val subsetWithK = subset.groupBy(QIDsUnionSA.head, QIDsUnionSA.tail: _*).count()
    val educationTree = taxonomyTreeJson.field(field).get
    val fullPathMap = mutable.Map[String, mutable.Map[String, mutable.Queue[String]]](field -> TopDownSpecialization.buildPathMapFromTree(educationTree))
    val anonymizationLevels: JsonArray = Json.array(Json("field" -> jString(field), "tree" -> educationTree)).arrayOrEmpty
    val anonymizedMap = TopDownSpecialization.anonymize(fullPathMap, QIDs, anonymizationLevels, subsetWithK, 5)
    val anonymized = TopDownSpecialization.generalize(anonymizedMap, subsetWithK, QIDs, 0)
    val kAnonymized = TopDownSpecialization.calculateK(anonymized, QIDsGeneralized)
    kAnonymized should be(8)
  }

}
