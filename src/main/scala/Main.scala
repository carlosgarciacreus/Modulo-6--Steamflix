import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import util.SparkCommon
import processor.ETLProcessor



object Main {
  val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    implicit val spark: SparkSession = SparkCommon.getSparkSession()

    ETLProcessor.run(
      inputPath + "/server_logs.txt",
      inputPath + "/movies_metadata.csv",
      outputPath
    )

  }


}
