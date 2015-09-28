package com.rafaelgarrote.learningspark.quickstart

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by rafaelgarrote on 12/3/15.
 */
object SimpleApp {

  def main(args: Array[String]) {
    val logFile = "src/resources/access_log"

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"------------- Lines with a: $numAs, Lines with b: $numBs --------------")

    sc.stop()
  }

}
