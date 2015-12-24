package org.witkon.extractJSON

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
