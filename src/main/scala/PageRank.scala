// scalastyle:off println
package com.Spark

// $example on$
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * A PageRank example on social network dataset
  * Run with
  * {{{
  * bin/run-example graphx.PageRankExample
  * }}}
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, args(0))
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile(args(1)).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    // $example off$

    val result = ranksByUsername.collect().sortBy(-_._2).take(100)

    // Print the top 100 result
    println("Top 100")
    println(result.mkString("\n"))

    spark.stop()
  }
}
