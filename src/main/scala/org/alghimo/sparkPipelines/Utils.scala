package org.alghimo.sparkPipelines

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import scala.collection.SeqLike

/**
  * Created by albert on 03/08/16.
  */
object Utils {

  def hashId(str: String): Long = {
    // This is effectively the same implementation as in Guava's Hashing, but 'inlined'
    // to avoid a dependency on Guava just for this. It creates a long from the first 8 bytes
    // of the (16 byte) MD5 hash, with first byte as least-significant byte in the long.
    val bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }

  def columnInList[Repr](col: String, existingColumns: SeqLike[String, Repr]): Column = {
    if (existingColumns.contains(col)) {
      new Column(col)
    } else {
      expr("NULL").as(col.toString)
    }
  }

  def withColor(str: String, color: String = Console.GREEN) = {
    color + str + Console.WHITE
  }
}