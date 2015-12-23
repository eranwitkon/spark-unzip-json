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


object outputOptions extends Enumeration {
  type outputOptions = Value
  val json,parquet,test = Value
}

class ExtractJSON {

  import outputOptions._

  def extractJSONFiles(sc: SparkContext, sqlContext: SQLContext,sourceFilesPath: String, destFilePath: String, output: outputOptions) = {
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

object extractJSONDriver{

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
    // /home/eranw/Workspace/sparkJsonSample/extractedOutput
    val outputOption = args(2).toLowerCase match {case "json" => outputOptions.json case "parquet" => outputOptions.parquet case "test" => outputOptions.test}
    val extractJsonArgs = new ExtractJsonArgs(args(0),args(1),outputOption)

    // disable generation of the metadata files
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    // disable the _SUCCESS file
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    ej.extractJSONFiles(sc, sqlContext, extractJsonArgs.source, extractJsonArgs.dest, outputOption)
  }
}