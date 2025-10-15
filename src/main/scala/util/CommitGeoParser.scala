package util

import util.Protocol.CommitGeo
import org.apache.flink.api.common.functions.MapFunction
import org.json4s.NoTypeHints
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.Serialization

/**
  * Parses a CommitGeo based on a JSON string.
  * You do NOT need to touch this file.
  */
class CommitGeoParser extends MapFunction[String, CommitGeo] {

  // Get deserialization formats.
  implicit lazy val formats = Serialization.formats(NoTypeHints) ++ JavaTimeSerializers.all

  /** Maps commit json to commit case class.*/
  override def map(value: String): CommitGeo = {
    Serialization.read[CommitGeo](value)
  }
}
