package com.rafaelgarrote.learningspark.exercices

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by rafaelgarrote on 28/9/15.
 */
object WordCountPurple {

//  Hacer de dos maneras distintas usando únicamente map, flatMap o filter el siguiente ejercicio:
//  Dar la palabra y el número de letras de las palabras que contengan la letra l del fichero purple

  lazy val purpleTextUrl = "src/resources/purple"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)
    sc.stop()
  }

  def firstExample(sc: SparkContext): Unit = {
    val file = sc.textFile(purpleTextUrl)
    val words = file.flatMap(_.split(" ")).filter(w => w.contains("l")).map(w => (w,w.length)).collect()
    println("------- Result ---------")
    words.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val file = sc.textFile(purpleTextUrl)
    val words = file.flatMap(_.split(" ")).flatMap(word => if(word.contains("l")) Seq((word, word.length)) else Seq()).collect()
    println("------- Result ---------")
    words.foreach(println)
  }

}
