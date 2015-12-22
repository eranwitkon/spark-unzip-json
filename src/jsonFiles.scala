import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

//Helper functions
object GZipHelper {

  def convertStreamToString(inStream: InputStream): String = {
    val s = new java.util.Scanner(inStream).useDelimiter("\\A");
    if (s.hasNext()) s.next() else "";
  }

  def compress(txt: String): Option[String] = {
    try {
      val arrOutputStream = new ByteArrayOutputStream()
      val zipOutputStream = new GZIPOutputStream(arrOutputStream)
      zipOutputStream.write(txt.getBytes)
      zipOutputStream.close()
      Some(Base64.getEncoder().encodeToString(arrOutputStream.toByteArray))
    } catch {
      case e: java.io.IOException => None
    }
  }


  def unCompress(deflatedTxt: String): Option[String] = {
    try {
      val bytes = Base64.getDecoder.decode(deflatedTxt)
      val zipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
      Some(convertStreamToString(zipInputStream))
    } catch {
      case e : java.io.IOException => None
    }
  }
}

class ExtractJSON {
  object outputFormat extends Enumeration {
    type outputFormat = Value
    val json,parquet = Value
  }

  import outputFormat._


  def extractJSONFiles(sc: SparkContext, sqlContext: SQLContext,sourceFilesPath: String, destFilePath: String, output: outputFormat) = {
    // Read all JSON files in a directory
    val jsonData = sqlContext.read.json(sourceFilesPath)
    // print the json schema
    //jsonData.printSchema()
    //loop through the DataFrame and manipulate the gzip Filed
    val jsonGzip = jsonData.map(r => Row(r.getString(0), GZipHelper.unCompress(r.getString(1)).get, r.getString(2), r.getString(3)))

    // The schema is encoded in a string
    val schemaString = "cty hse nm yrs"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val unzipJSON = sqlContext.createDataFrame(jsonGzip, schema)
    val outputFileName = "extracted-"
    output match {
      case outputFormat.json => unzipJSON.coalesce(1).write.mode("Overwrite").json(destFilePath + outputFileName + outputFormat.json)
      case outputFormat.parquet => unzipJSON.write.mode("Overwrite").parquet(destFilePath + outputFileName + outputFormat.parquet)
    }

  }
}

object extractJSONDriver{
  def main(agrs: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark unZip JSON")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    // disable generation of the metadata files
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    // disable the _SUCCESS file
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val ej = new ExtractJSON()

    ej.extractJSONFiles(sc,sqlContext,"/home/eranw/Workspace/sparkJsonSample/gzipSample","/home/eranw/Workspace/sparkJsonSample/extractedOutput/",ej.outputFormat.json)
    ej.extractJSONFiles(sc,sqlContext,"/home/eranw/Workspace/sparkJsonSample/gzipSample","/home/eranw/Workspace/sparkJsonSample/extractedOutput/",ej.outputFormat.parquet)
  }
}