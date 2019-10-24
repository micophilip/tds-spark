import org.scalatest.FunSuite
import scala.io.Source
import argonaut.Argonaut._
import argonaut._

class TopDownSpecializationTest extends FunSuite {

  test("findAncestor should return correct ancestor") {
    val resource = Source.fromResource("taxonomytree.json")
    val taxonomyTree = try resource.mkString finally resource.close()
    val taxonomyTreeJson = taxonomyTree.parseOption.get.field("education").get
    assert(TopDownSpecialization.findAncestor(taxonomyTreeJson, "5th-6th") == "Any")
    assert(TopDownSpecialization.findAncestor(taxonomyTreeJson.field("leaves").get.arrayOrEmpty.head, "10th") == "Without-Post-Secondary")
    assert(TopDownSpecialization.findAncestor(taxonomyTreeJson.field("leaves").get.arrayOrEmpty.tail.head, "University") == "Post-secondary")
  }

}
