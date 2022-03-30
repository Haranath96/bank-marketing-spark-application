package common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {

  private val logger =LoggerFactory.getLogger(getClass.getName)


  def createSparkSession(): Option[SparkSession] = {

    try {
      System.setProperty("hadoop.home.dir", "C:/app/hadoop")

      val sparkSession = SparkSession.builder()
        .appName("Bank Marketing Spark App")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      logger.info("Created Spark Session")
      Some(sparkSession)


    }
    catch {
      case e: Exception =>
        System.exit(1)
        None
    }

  }
}
