import argonaut.Argonaut._
import argonaut._
import org.scalactic.Tolerance._
import org.scalatest._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.{BufferedSource, Source}

class TopDownSpecializationTest extends FunSuite with BeforeAndAfterAll {

  val resource: BufferedSource = Source.fromResource("taxonomytree.json")
  val taxonomyTree: String = try resource.mkString finally resource.close()
  val taxonomyTreeJson: Json = taxonomyTree.parseOption.get

  val spark: SparkSession = TopDownSpecialization.spark

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

    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree), "5th-6th", 0).contains("Any"))
    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "10th", 0).contains("Without-Post-Secondary"))
    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "Preschool", 0).contains("Without-Post-Secondary"))

    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head), "Assoc-voc ", 0).contains("Post-secondary"))

    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), "University", 0).isEmpty)
    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head), "Elementary", 0).isEmpty)

    assert(TopDownSpecialization.findAncestor(TopDownSpecialization.buildPathMapFromTree(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head), null, 0).isEmpty)
  }

  test("calculateScore should accurately calculate entropy") {

    val QIDs = List("education", "sex", "work_hrs", "class")

    val subset = input.select(QIDs.head, QIDs.tail: _*)
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    val score = TopDownSpecialization.calculateScore(TopDownSpecialization.buildPathMapFromTree(taxonomyTreeJson.field("education").get), taxonomyTreeJson, subsetWithK, "class", List("<=50", ">50"), "education")

    assert(score === 0.0151 +- 0.001)

  }

  test("calculateK should accurately calculate k") {

    val QIDs = List("sex", "work_hrs")

    val subset = input.select(QIDs.head, QIDs.tail: _*)
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    assert(TopDownSpecialization.calculateK(subsetWithK, QIDs) == 3)
  }

}
