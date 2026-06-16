package util

import org.apache.spark.sql.SparkSession

object SparkCommon {

  def getSparkSession() = {
     SparkSession.builder()
      .appName("Modulo 6")
      .master("local[*]")
      .getOrCreate()
  }

}
