import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TopDownSpecialization extends Serializable {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("TopDownSpecialization").getOrCreate()
  }
}
