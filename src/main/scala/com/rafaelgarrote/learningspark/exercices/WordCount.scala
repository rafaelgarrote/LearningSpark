package com.rafaelgarrote.learningspark.exercices

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rafaelgarrote on 28/9/15.
 */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val logFile = "src/resources/words"

    val file = sc.textFile(logFile)

    val totalWords = file.flatMap(line => line.split(" "))
    val words = totalWords.map(word => (word, 1)).reduceByKey(_ + _).collect()

    println(s"-- Total words: ${totalWords.count()}")
    println(s"-- Word occurrence: " )
    words.foreach(println)

    sc.stop()
  }

}
