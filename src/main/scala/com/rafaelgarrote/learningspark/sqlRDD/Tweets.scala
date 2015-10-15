package com.rafaelgarrote.learningspark.sqlRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by rgarrote on 14/10/15.
 */

object Tweets {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Querying JSON File")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)

    val input = hiveCtx.jsonFile("src/resources/testweet.json")
    // Register the input schema RDD
    input.registerTempTable("tweets")

    println("--------------- Tweet schema:")
    input.printSchema()
    // Select tweets based on the retweetCount
    val topTweets: SchemaRDD = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")

    val topTweetText = topTweets.map(row => row.getString(0))

    val texts = topTweetText.collect()
    println("---------------- Top Tweets:")
    texts.foreach(println)

    sc.stop()
  }

}
