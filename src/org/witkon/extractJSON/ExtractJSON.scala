package org.witkon.extractJSON

import org.apache.spark._
import org.apache.spark.sql._



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
    val jsonUnGzip = jsonData.map(r => Row(r.getString(0), GZipHelper.unCompress(r.getString(1)).get, r.getString(2), r.getString(3)))

    // construct a nested JSON and pars it
    val jsonNested = sqlContext.read.json(jsonUnGzip.map{case Row(cty:String, json:String,nm:String,yrs:String) => s"""{"cty": \"$cty\", "extractedJson": $json , "nm": \"$nm\" , "yrs": \"$yrs\"}"""})

    val outputFileName = destFilePath + "extracted-" + output
    output match {
      case outputOptions.json => jsonNested.coalesce(1).write.mode("Overwrite").json(outputFileName )
      case outputOptions.parquet => jsonNested.write.mode("Overwrite").parquet(outputFileName)
      case outputOptions.test =>
        println("**** SOURCE schema ****")
        jsonData.printSchema()
        println("**** DEST schema ****")
        jsonNested.printSchema()
    }
  }
}
