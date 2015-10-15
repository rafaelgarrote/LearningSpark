package com.rafaelgarrote.learningspark.sqlRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by rgarrote on 15/10/15.
 */
object QueryingFromJSON {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Querying from RDD")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "src/resources/people.json"

    // Create a SchemaRDD from the file(s) pointed to by path
    val people = sqlContext.jsonFile(path)

    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()

    // Register this SchemaRDD as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    teenagers.map(t => "Name: " + t.head).collect().foreach(println)

    // Alternatively, a SchemaRDD can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)

    anotherPeople.printSchema()

    anotherPeople.registerTempTable("anotherPeople")

    val anotherResult = sqlContext.sql("SELECT name FROM anotherPeople")

    anotherResult.map(t => "Name: " + t.head).collect().foreach(println)

    sc.stop()

  }
}
