package com.Spark

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // Load the page id links as a graph
    val graph = GraphLoader.edgeListFile(sc, args(0))
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the page id-title pair
    val users = sc.textFile(args(1)).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // get top 100 result based on rank
    val result = ranksByUsername.collect().sortBy(-_._2).take(100)

    // Print the top 100 result
    println("Top 100")
    println(result.mkString("\n"))

    spark.stop()
  }
}
