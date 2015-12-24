/**
  * Created by eranw on 24/12/15.
  */

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
  * Created by eranw on 23/12/15.
  */
object GZipHelper extends java.io.Serializable{
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

// disable generation of the metadata files
sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
// disable the _SUCCESS file
sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

import org.apache.spark._
import org.apache.spark.sql._

val sourceFilesPath = "/home/eranw/Workspace/sparkJsonSample/gzipSample"
val jsonData = sqlContext.read.json(sourceFilesPath)
val jsonUnGzip = jsonData.map(r => Row(r.getString(0), GZipHelper.unCompress(r.getString(1)).get, r.getString(2), r.getString(3)))

val jsonNested = sqlContext.read.json(jsonUnGzip.map{case Row(cty:String, json:String,nm:String,yrs:String) => s"""{"cty": \"$cty\", "extractedJson": $json , "nm": \"$nm\" , "yrs": \"$yrs\"}"""})

val output = "json"
val destFilePath = "/home/eranw/Workspace/sparkJsonSample/extractedOutput/"
val outputFileName = destFilePath + "extracted-" + output
jsonNested.coalesce(1).write.mode("Overwrite").json(outputFileName )

