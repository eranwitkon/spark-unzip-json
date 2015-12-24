package org.witkon.extractJSON

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by eranw on 23/12/15.
  */
object extractJSONDriver {
  import outputOptions._

  def main(args: Array[String]): Unit = {

    // to be used in spark-submit only
    // Don't paste this code to spark-shell
    val conf = new SparkConf().setAppName("Spark unZip JSON")
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.eventLog.dir","/usr/local/spark/history/log")
    conf.set("spark.driver.memory","5G")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // End of spark-submit only

    val ej = new ExtractJSON()
    case class ExtractJsonArgs (source: String,dest: String,options: outputOptions)
    // /home/eranw/Workspace/sparkJsonSample/gzipSample
    // /home/eranw/Workspace/sparkJsonSample/extractedOutput/
    val outputOption = args(2).toLowerCase match {case "json" => outputOptions.json case "parquet" => outputOptions.parquet case "test" => outputOptions.test}
    val extractJsonArgs = new ExtractJsonArgs(args(0),args(1),outputOption)

    // disable generation of the metadata files
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    // disable the _SUCCESS file
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    ej.extractJSONFiles(sc, sqlContext, extractJsonArgs.source, extractJsonArgs.dest, outputOption)
  }
}
