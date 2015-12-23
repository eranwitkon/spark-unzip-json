package org.witkon.extractJSON

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object outputOptions extends Enumeration {
  type outputOptions = Value
  val json,parquet,test = Value
}

class ExtractJSON {

  import outputOptions._

  def extractJSONFiles(sc: SparkContext, sqlContext: SQLContext,sourceFilesPath: String, destFilePath: String, output: outputOptions) = {
    // Read all JSON files in a directory
    val jsonData = sqlContext.read.json(sourceFilesPath)

    //loop through the DataFrame and manipulate the gzip Filed
    val jsonGzip = jsonData.map(r => Row(r.getString(0), GZipHelper.unCompress(r.getString(1)).get, r.getString(2), r.getString(3)))

    // The schema is encoded in a string
    val schemaString = "cty hse nm yrs"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val unzipJSON = sqlContext.createDataFrame(jsonGzip, schema)
    val outputFileName = destFilePath + "extracted-" + output
    output match {
      case outputOptions.json => unzipJSON.coalesce(1).write.mode("Overwrite").json(outputFileName )
      case outputOptions.parquet => unzipJSON.write.mode("Overwrite").parquet(outputFileName)
      case outputOptions.test =>
        println("**** SOURCE schema ****")
        jsonData.printSchema()
        println("**** DEST schema ****")
        unzipJSON.printSchema()
    }
  }
}
