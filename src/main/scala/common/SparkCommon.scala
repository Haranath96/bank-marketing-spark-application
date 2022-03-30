package common

import org.apache.spark.sql.{DataFrame, SparkSession}
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

  def createHiveTableUsingSparkSession(spark : SparkSession) : Unit = {
    spark.sql("create database if not exists sampledb")
    spark.sql("use sampledb")
    spark.sql("""create table if not exists sample_table12 (course_id string, course_name string, author_name string,no_of_reviews string)""")
    spark.sql("insert into sampledb.sample_table12  VALUES (1,'Java','FutureXSkill',56)")
    spark.sql("insert into sampledb.sample_table12 VALUES (2,'Java','FutureXSkill',56)")
    spark.sql("insert into sampledb.sample_table12 VALUES (3,'Big Data','Future',100)")
    spark.sql("insert into sampledb.sample_table12 VALUES (4,'Linux','Future',100)")
    spark.sql("insert into sampledb.sample_table12 VALUES (5,'Microservices','Future',100)")
    spark.sql("insert into sampledb.sample_table12 VALUES (6,'CMS','',100)")
    spark.sql("insert into sampledb.sample_table12 VALUES (7,'Python','FutureX','')")
    spark.sql("insert into sampledb.sample_table12 VALUES (8,'CMS','Future',56)")
    spark.sql("insert into sampledb.sample_table12 VALUES (9,'Dot Net','FutureXSkill',34)")
    spark.sql("insert into sampledb.sample_table12 VALUES (10,'Ansible','FutureX',123)")
    spark.sql("insert into sampledb.sample_table12 VALUES (11,'Jenkins','Future',32)")
    spark.sql("insert into sampledb.sample_table12 VALUES (12,'Chef','FutureX',121)")
    spark.sql("insert into sampledb.sample_table12 VALUES (13,'Go Lang','',105)")

    spark.sql("alter table sampledb.sample_table12 set tblproperties('serialization.null.format'='')")

  }


  def readHiveTable(spark : SparkSession) : Option[DataFrame] = {
    try {
      logger.warn("readHiveTable method started")
      val courseDF = spark.sql("select * from sampledb.sample_table12")
      logger.warn("readHiveTable method ended")
      Some(courseDF)
    } catch {
      case e: Exception =>
        logger.error("Error Reading fxxcoursedb.fx_course_table "+e.printStackTrace())
        None
    }
  }

  def writeToHiveTable(spark:SparkSession, transformedDF: DataFrame, hiveTableName: String) :Unit ={
    try {
      logger.warn("writeToHiveTable started")

      val tmpView = hiveTableName+"tempView"
      transformedDF.createOrReplaceTempView(tmpView)
      //

      val sqlQuery = "create table sampledb."+ hiveTableName + " as select * from "+ tmpView

      spark.sql(sqlQuery)
      logger.warn("Finished writing to Hive Table")

    } catch {
      case e: Exception =>
        logger.error("Error writing to Hive Table"+e.printStackTrace())
    }
  }


}
