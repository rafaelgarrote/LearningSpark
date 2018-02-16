package com.rafaelgarrote.learningspark.sqlRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StructType

/**
 * Created by rgarrote on 14/10/15.
 */
object QueryingFromRDD {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Querying from RDD")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val people = sc.textFile("src/resources/people.txt")

    // Using schema string to define the schema
    firstExample(people, sqlContext)

    // Using a case class to define the schema
    secondExample(people, sqlContext)

    sc.stop()
  }


  def firstExample(peopleRDD: RDD[String], sqlContext: SQLContext): Unit = {
    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
      val rowRDD: RDD[Row] = peopleRDD.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "Name: " + t.head).collect().foreach(println)
  }

  def secondExample(peopleRDD: RDD[String], sqlContext: SQLContext): Unit = {
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    // Create an RDD of Person objects and register it as a table.
    val people: RDD[Person] = peopleRDD.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.registerTempTable("people2")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people2 WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // Importing the SQL context gives access to all the public SQL functions and implicit conversions.
    import sqlContext._
    // The following is the same as 'SELECT name FROM people WHERE age >= 10 AND age <= 19'
    val adults = people.where('age > 19).select('name)
    adults.map(t => "Name: " + t(0)).collect().foreach(println)
  }

}

case class Person(name: String, age: Int)
