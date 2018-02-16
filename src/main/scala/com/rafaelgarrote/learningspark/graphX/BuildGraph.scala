package com.rafaelgarrote.learningspark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

/**
 * Created by rgarrote on 28/02/16.
 */
object BuildGraph {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val metroFileUrl = "src/resources/EOADATA/metro.csv"
    val countryFileUrl = "src/resources/EOADATA/country.csv"
    val metroCountryFileUrl = "src/resources/EOADATA/metro_country.csv"

    val metroFile = sc.textFile(metroFileUrl)
    val countryFile = sc.textFile(countryFileUrl)
    val metroCountryFile = sc.textFile(metroCountryFileUrl)

    // Before importing any datasets, let view what the files contain.
    println("---------------- File content: ------------------")
    println("Metros:")
    metroFile.take(5).foreach(println)
    println("Countries:")
    countryFile.take(5).foreach(println)
    println("Relationships")
    metroCountryFile.take(5).foreach(println)

    // IMPORT NODES

    // Read the comma delimited text file metros.csv into an RDD of Metro vertices, ignore
    // lines that start with # and map the columns to: id, Metro(name, population).
    val metros: RDD[(VertexId, PlaceNode)] =
      metroFile     .filter(! _.startsWith("#")).
        map {line =>
          val row = line split ','
          (0L + row(0).toInt, Metro(row(1), row(2).toInt))
        }

    // Read the comma delimited text file country.csv into an RDD of Country vertices, ignore
    // lines that start with # and map the columns to: id, Country(name). Add 100 to the
    // country indexes so they are unique from the metro indexes.
    val countries: RDD[(VertexId, PlaceNode)] =
      countryFile.
        filter(! _.startsWith("#")).
        map {line =>
          val row = line split ','
          (100L + row(0).toInt, Country(row(1)))
        }

    // IMPORT VERTICES

    // Read the comma delimited text file metro_country.tsv into an RDD[Edge[Int]] collection.
    // Remember to add 100 to the countries' vertex id.
    val mclinks: RDD[Edge[Int]] =
      metroCountryFile.
        filter(! _.startsWith("#")).
        map {line =>
          val row = line split ','
          Edge(0L + row(0).toInt, 100L + row(1).toInt, 1)
        }

    // CREATE A GRAPH

    // Concatenate the two sets of nodes into a single RDD.
    val nodes = metros ++ countries

    // Pass the concatenated RDD to the Graph() factory method along with the RDD link
    val metrosGraph = Graph(nodes, mclinks)

    // Print the first 5 vertices and edges.
    println("---------------- First 5 vertices and edges: ------------------")
    metrosGraph.vertices.take(5)

    // Filter all of the edges in metrosGraph that have a source vertex Id of 1 and create a
    // map of destination vertex Ids.
    metrosGraph.edges.filter(_.srcId == 1).map(_.dstId).collect()

    // Filter all of the edges in metrosGraph where the destination vertexId is 103
    // and create a map of all of the source Ids
    metrosGraph.edges.filter(_.dstId == 103).map(_.srcId).collect()

    sc.stop()
  }

}

// Create case classes for the places (metros and countries).
class PlaceNode(val name: String) extends Serializable
case class Metro(override val name: String, population: Int) extends PlaceNode(name)
case class Country(override val name: String) extends PlaceNode(name)
