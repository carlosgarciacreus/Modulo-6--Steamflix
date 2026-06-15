import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.expressions.Window



object Main {
  val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    try {
      // SparkSession
      val spark = SparkSession.builder()
        .appName("Modulo 6")
        .master("local[*]")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")


      logger.info("Leyendo logs...")

      // Lectura de ficheros
      val logsDF = spark.read.text(inputPath + "/server_logs.txt")
      val moviesDF = spark.read.option("header", "true").csv(inputPath + "/movies_metadata.csv")

      // Limpieza

      val logsLimpio = logsDF.filter(col("value").startsWith("[INFO]"))

      val logsExtraido = logsLimpio
        .withColumn("parts", split(col("value"), "\\|"))
        .withColumn("inicio", to_timestamp(regexp_replace(col("parts").getItem(0), "\\[INFO\\] ", ""), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("usuario", split(col("parts").getItem(1), ":").getItem(1))
        .withColumn("movie_id", regexp_replace(split(col("parts").getItem(2), ":").getItem(1), "Movie_", ""))
        .withColumn("duracion", split(col("parts").getItem(3), ":").getItem(1))
        .drop("value", "parts")

      logsExtraido.show(10)

      val moviesLimpio = moviesDF.withColumn("subscription_price",
        regexp_replace(col("subscription_price"), "\\$", "").cast("Double"))
        .withColumn("genres",
          when(col("genres").isNull || col("genres") === "", "Unknown")
            .otherwise(col("genres")))

      logger.info("Logs limpios")

      // Join
      val dfUnido = logsExtraido.join(
        broadcast(moviesLimpio),
        logsExtraido("movie_id") === moviesLimpio("id"),
        "inner"
      ).drop("movie_id")

      // KPIs

      val topGenre = dfUnido
        .withColumn("genre", explode(split(col("genres"), "\\|")))
        .groupBy("genre")
        .agg(sum("duracion").alias("total_hours"))

      val windowSpec = Window.partitionBy("usuario").orderBy("inicio")

      val isBinge = logsExtraido
        .withColumn("prev_inicio", lag("inicio", 1).over(windowSpec))
        .withColumn("diferencia", (col("inicio").cast(LongType) - col("prev_inicio").cast(LongType)) / 60)
        .withColumn("is_binge", when(col("diferencia") > 0 && col("diferencia") < 20, 1).otherwise(0))

      val topBinge = isBinge.filter(col("is_binge") === 1).groupBy("usuario").agg(count("*").alias("conteo")).orderBy(col("conteo").desc).limit(10)

      logger.info("Guardando en Parquet...")
      // Guardar en Parquet
      dfUnido.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)

      logger.info("Job completado")

    } catch {
      case e: Exception => logger.error("Error: " + e.getMessage)
    }
  }
}
