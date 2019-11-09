import TopDownSpecialization.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object ExpandDataset extends Serializable {

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val outputFolder = args(1)
    val originalCount = args(2).toInt
    val expandTo = args(3).toInt

    val input = spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .csv(path)

    var data = ListBuffer[Row]()
    val target = expandTo - originalCount

    val allowNull = false

    val marital_status = List("Separated", "Never-married", "Married-spouse-absent", "Divorced", "Widowed", "Married-AF-spouse", "Married-civ-spouse")
    val occupation = List("Sales", "Exec-managerial", "Prof-specialty", "Handlers-cleaners", "Farming-fishing", "Craft-repair", "Transport-moving", "Priv-house-serv", "Protective-serv", "Other-service", "Tech-support", "Machine-op-inspct", "Armed-Forces", "Adm-clerical")
    val native_country = List("Philippines", "Germany", "Cambodia", "France", "Greece", "Taiwan", "null", "Ecuador", "Nicaragua", "Hong", "Peru", "India", "China", "Italy", "Holand-Netherlands", "Cuba", "South", "Iran", "Ireland", "Thailand", "Laos", "El-Salvador", "Mexico", "Guatemala", "Honduras", "Yugoslavia", "Puerto-Rico", "Jamaica", "Canada", "United-States", "Dominican-Republic", "Outlying-US(Guam-USVI-etc)", "Japan", "England", "Haiti", "Poland", "Portugal", "Columbia", "Scotland", "Hungary", "Vietnam", "Trinadad&Tobago")
    val workclass = List("Self-emp-not-inc", "null", "Local-gov", "State-gov", "Private", "Without-pay", "Federal-gov", "Never-worked", "Self-emp-inc")
    val relationship = List("Own-child", "Not-in-family", "Unmarried", "Wife", "Other-relative", "Husband")
    val sex = List("Male", "Female")
    val income = List("<=50K", ">50K")
    val race = List("Other", "Amer-Indian-Eskimo", "White", "Asian-Pac-Islander", "Black")
    val educationNum = Map(13 -> "Bachelors",
      9 -> "HS-grad",
      7 -> "11th",
      14 -> "Masters",
      5 -> "9th",
      10 -> "Some-college",
      12 -> "Assoc-acdm",
      11 -> "Assoc-voc",
      4 -> "7th-8th",
      16 -> "Doctorate",
      15 -> "Prof-school",
      3 -> "5th-6th",
      6 -> "10th",
      2 -> "1st-4th",
      1 -> "Preschool",
      8 -> "12th")

    for (_ <- 0.to(target)) {
      val ageCol: Int = getRandom(17, 90)
      val educationNumCol = if (ageCol < 27) getRandom(1, 12) else getRandom(1, 16)
      val educationCol = educationNum(educationNumCol)
      val relationshipCol = relationship(getRandom(0, relationship.length - 1))
      val sexCol = relationshipCol match {
        case "Wife" => "Female"
        case "Husband" => "Male"
        case _ => sex(getRandom(0, sex.length - 1))
      }

      data += Row(ageCol, workclass(getRandom(0, workclass.length - 1)), getRandom(12285, 1484705), educationCol, educationNumCol,
        marital_status(getRandom(0, marital_status.length - 1)), occupation(getRandom(0, occupation.length - 1)),
        relationshipCol, race(getRandom(0, race.length - 1)), sexCol,
        getRandom(0, 99999), getRandom(0, 4356), getRandom(1, 99), native_country(getRandom(0, native_country.length - 1)), income(getRandom(0, income.length - 1)))
    }

    val schema = List(
      StructField("age", IntegerType, allowNull),
      StructField("workclass", StringType, allowNull),
      StructField("fnlwgt", IntegerType, allowNull),
      StructField("education", StringType, allowNull),
      StructField("education-num", IntegerType, allowNull),
      StructField("marital_status", StringType, allowNull),
      StructField("occupation", StringType, allowNull),
      StructField("relationship", StringType, allowNull),
      StructField("race", StringType, allowNull),
      StructField("sex", StringType, allowNull),
      StructField("capital-gain", IntegerType, allowNull),
      StructField("capital-loss", IntegerType, allowNull),
      StructField("hours-per-week", IntegerType, allowNull),
      StructField("native_country", StringType, allowNull),
      StructField("income", StringType, allowNull)
    )

    val output = spark.createDataFrame(spark.sparkContext.parallelize(data.toList), StructType(schema)).union(input)

    output.coalesce(1).write.option("header", "true").csv(outputFolder)

    spark.stop()

    println(s"generated ${data.length}")
  }

  def getRandom(start: Int, end: Int): Int = {
    start + new scala.util.Random().nextInt((end - start) + 1)
  }
}
