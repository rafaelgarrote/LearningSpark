package com.rafaelgarrote.learningspark.baseRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by rgarrote on 28/09/15.
 */
object Aggregate {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val z = sc.parallelize(List(1,2,3,4,5,6), 2)

    z.mapPartitionsWithIndex(myfunc).collect().foreach(println)

    println(z.aggregate(0)(math.max(_,_), _ + _))

    println(z.aggregate(5)(math.max(_,_), _ + _))

    sc.stop()
  }

  // lets first print out the contents of the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }

}
