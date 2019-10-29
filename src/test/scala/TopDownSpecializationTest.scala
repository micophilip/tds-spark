import argonaut.Argonaut._
import org.scalactic.Tolerance._
import org.scalatest._

import scala.io.Source

class TopDownSpecializationTest extends FunSuite with BeforeAndAfterAll {

  val resource = Source.fromResource("taxonomytree.json")
  val taxonomyTree = try resource.mkString finally resource.close()
  val taxonomyTreeJson = taxonomyTree.parseOption.get

  val spark = TopDownSpecialization.spark

  override def afterAll {
    spark.stop()
  }

  test("findAncestor should return correct ancestor") {

    val educationTaxonomyTree = taxonomyTreeJson.field("education").get

    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree, "5th-6th").contains("Any"))
    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head, "10th").contains("Without-Post-Secondary"))
    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head, "Preschool").contains("Without-Post-Secondary"))

    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head, "Assoc-voc ").contains("Post-secondary"))

    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head, "University").isEmpty)
    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.tail.head, "Elementary").isEmpty)

    assert(TopDownSpecialization.findAncestor(educationTaxonomyTree.field("leaves").get.arrayOrEmpty.head, null).isEmpty)
  }

  test("calculateEntropy should accurately calculate entropy") {

    val testData = getClass.getResource("/test_data.csv").getPath

    val input = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .csv(testData)

    val QIDs = List("education", "sex", "work_hrs", "class")

    val subset = input.select(QIDs.head, QIDs.tail: _*)
    val subsetWithK = subset.groupBy(QIDs.head, QIDs.tail: _*).count()

    val score = TopDownSpecialization.calculateScore(taxonomyTreeJson, subsetWithK, "class", List("<=50", ">50"))

    assert(score === 0.0151 +- 0.001)

  }

}
