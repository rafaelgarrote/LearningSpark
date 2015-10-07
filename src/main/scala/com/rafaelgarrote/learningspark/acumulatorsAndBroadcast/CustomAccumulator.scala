package com.rafaelgarrote.learningspark.acumulatorsAndBroadcast

import org.apache.spark.AccumulableParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * Created by rgarrote on 7/10/15.
 */
object CustomAccumulator {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    implicit val param = new AccumulableMap()
    val empAccu = sc.accumulable(Map[Int, Employee]())
    val employees = List(
      Employee(10001, "Tom", "Eng"),
      Employee(10002, "Roger", "Sales"),
      Employee(10003, "Rafael", "Sales"))
    sc.parallelize(employees).foreach(e => empAccu += e)
    println("--------------Employees: ")
    empAccu.value.foreach(entry => println(s"emp id = ${entry._1} name = ${entry._2.name}"))

    sc.stop()
  }
}

case class Employee(id: Int, name: String, dep: String)

class AccumulableMap extends AccumulableParam[Map[Int, Employee], Employee] {

  override def addAccumulator(r: Map[Int, Employee], t: Employee): Map[Int, Employee] = r + (t.id -> t)

  override def addInPlace(r1: Map[Int, Employee], r2: Map[Int, Employee]): Map[Int, Employee] = r1 ++ r2

  override def zero(initialValue: Map[Int, Employee]): Map[Int, Employee] = Map[Int, Employee]()

}
