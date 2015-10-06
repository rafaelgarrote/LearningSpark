package com.rafaelgarrote.learningspark.acumulatorsAndBroadcast

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

/**
 * Created by rgarrote on 6/10/15.
 */
object Accumulator {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val logFile = sc.textFile("src/resources/error_log")

    val blankLines = sc.accumulator[Int](0)

    val words = logFile.flatMap(line => {
      if(line == "") {blankLines += 1}
      line.split(" ")
    }).collect()

    println(s"----- Blank lines: ${blankLines.value}")
    sc.stop()
  }
}
