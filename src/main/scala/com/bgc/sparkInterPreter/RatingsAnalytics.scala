package com.bgc.sparkInterPreter


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RatingsAnalytics {


  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C://hadoop");
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val dataFrameReader = session.read

    val ratingsDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/ratings.tsv")

    val nameBasicsDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/name_basic.tsv")

    val titleBasicsDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/title_basics.tsv")

    val principalsDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/principals.tsv")

    val akasTitleDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/akas_title.tsv")

    val crewTitleDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/crew_title.tsv")

    val episodeTitleDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .option("sep", "\t")
      .csv("in/episode_title.tsv")

    System.out.println("=== Print out schema ===")
    //ratingsDF.printSchema()
    //titleBasicsDF.printSchema()
    //nameBasicsDF.printSchema()
   // principalsDF.printSchema()
    //crewTitleDF.printSchema()
    //episodeTitleDF.printSchema()
    //akasTitleDF.printSchema()
     System.out.println("=== Q1 List of top 20 Movies with a minimum of 50 votes and Raking ===")
     val filterRatingDF =  ratingsDF.withColumn("Ranking",(col("numVotes").divide(col("averageRating"))).multiply(col("averageRating")))
     filterRatingDF.where(col("numVotes") >= 50).orderBy(col("numVotes"),col("Ranking").desc)

    val joined = filterRatingDF.join(titleBasicsDF,filterRatingDF("tconst") === titleBasicsDF("tconst"),"inner")
    val finalDS = joined.withColumn("finalKey",filterRatingDF("tconst"))
                         .withColumn("Primary_Title",col("primaryTitle"))
                         .withColumn("Original_Title",col("originalTitle"))
                         .withColumn("Number_Votes",col("numVotes"))
                         .withColumn("Rankings",col("Ranking"))
      .where(col("numVotes") >= 50).orderBy(col("numVotes"),col("Ranking").desc)

      val finalDSWithLimit = finalDS.select("finalKey","Primary_Title","Original_Title","Number_Votes","Rankings").limit(20)
      finalDSWithLimit.show()
     System.out.println("=== Q2 List of top 20 Movies with a minimum of 50 votes and Raking ===")
    val DSWithPerson = finalDSWithLimit.join(principalsDF,finalDSWithLimit("finalKey") ===principalsDF("tconst"),"inner")
    val finalDSWithPerson = DSWithPerson.join(nameBasicsDF,DSWithPerson("nconst") === nameBasicsDF("nconst"))
    finalDSWithPerson.show()

    session.stop()
  }
}
