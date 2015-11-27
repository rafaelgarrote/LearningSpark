package com.rafaelgarrote.learningspark.exercices

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rgarrote on 27/11/15.
 */
object SimpleJoin2 {

  lazy val channelFilesPathUrl = "src/resources/join2_genchan?.txt"
  lazy val countFilesPathUrl = "src/resources/join2_gennum?.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Join 2")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val channels = sc.textFile(channelFilesPathUrl)
    val views = sc.textFile(countFilesPathUrl)

    val show_views = views.map(splitShowViews)
    val show_channel = channels.map(splitShowChannel)

    //(Show, (Channel, Views))
    val joinedDataSet: RDD[(String, (String, Int))] = show_channel.join(show_views)
    val channelViews = joinedDataSet.map(extractChannelViews)

    val result = channelViews.reduceByKey(_+_).collect()
    result.foreach(println)

    sc.stop()
  }

  def splitShowViews(line: String): (String, Int) = {
    val showViews = line.split(",")
    val show = showViews(0)
    val views = showViews(1).replace(" ", "").toInt
    (show, views)
  }

  def splitShowChannel(line: String): (String, String) = {
    val showChannel = line.split(",")
    val show = showChannel(0)
    val channel = showChannel(1)
    (show, channel)
  }

  def extractChannelViews(show_views_channel: (String, (String, Int))): (String, Int) = show_views_channel._2


}
