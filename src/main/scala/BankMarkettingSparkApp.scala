import common.{SparkCommon, SparkTransformer}
import org.slf4j.LoggerFactory

object BankMarkettingSparkApp {

  private val logger =LoggerFactory.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {

    logger.info("main method started")
    val spark = SparkCommon.createSparkSession().get

    //val sampleSeq= Seq((1,"spark"),(2,"Big Data"))

   // val df = spark.createDataFrame(sampleSeq).toDF("courseId","course Names")
    //df.write.format(source="csv").save(path="sampleSeq")

    //SparkCommon.createHiveTableUsingSparkSession(spark)

    val courseDF = SparkCommon.readHiveTable(spark).get
    courseDF.show()

    val transformedDF = SparkTransformer.replaceNullValues(courseDF)
    transformedDF.show()

    SparkCommon.writeToHiveTable(spark,transformedDF,"customer_transformed")
    logger.info("Finished writing to Hive Table..in main method")
  }



}
