import common.SparkCommon
import org.slf4j.LoggerFactory

object BankMarkettingSparkApp {

  private val logger =LoggerFactory.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {

    logger.info("main method started")
    val spark = SparkCommon.createSparkSession().get

    val sampleSeq= Seq((1,"spark"),(2,"Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("courseId","course Names")
    df.write.format(source="csv").save(path="sampleSeq")
  }

}
