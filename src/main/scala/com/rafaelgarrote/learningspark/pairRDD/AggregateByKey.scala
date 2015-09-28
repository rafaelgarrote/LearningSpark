package com.rafaelgarrote.learningspark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by rafaelgarrote on 28/9/15.
 */
object AggregateByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)

    sc.stop()
  }

  // lets have a look at what is in the partitions
  def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val pairRDD: RDD[(String, Int)] =
      sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    val spartitions = pairRDD.mapPartitionsWithIndex(myfunc).collect()

    val result0: Array[(String, Int)] = pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect()
    val result100 = pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect()

    println("---------- Example 1 ----------")
    spartitions.foreach(println)
    println(s"-- ZeroValue: 0, Result:")
    result0.foreach(println)
    println(s"-- ZeroValue: 100, Result:")
    result100.foreach(println)
  }
}
