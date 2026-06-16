package processor

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{broadcast, col, count, explode, lag, regexp_replace, split, sum, to_timestamp, when, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import config.Constants
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

object ETLProcessor {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def run(rawLogsPath: String, moviesPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    // 1. Leer logs y movies

    val rawLogsDF =  readRawLogsWithTry(rawLogsPath)
    val rawMoviesDF = readMoviesWithTry(moviesPath)

    // 2. Limpiar y estructurar Logs
    logger.info("Procesando y limpiando logs...")

    val validLogsDF = validLogs(rawLogsDF)
    val logsExtraidoDF = logsExtraido(validLogsDF)
    logsExtraidoDF.cache()

    logger.info(s"Logs de reproducción limpios cargados: ${logsExtraidoDF.count()} registros")

    // 3. Limpiar y estructurar catálogo de películas

    logger.info("Procesando y estandarizando catálogo de películas...")

    val movieLimpioDF = movieLimpio(rawMoviesDF)
    movieLimpioDF.cache()

    // 4. Enriquecer datos con Broadcast Join

    logger.info("Enriqueciendo reproducciones con metadatos de catálogo (Broadcast Join)...")
    val enrichedDF = joinLogsWithMovies(logsExtraidoDF, movieLimpioDF)
    enrichedDF.cache()

    logger.info(s"Registros resultantes del Join: ${enrichedDF.count()} registros")

    // 5. Calcular KPIs (Top Géneros y Binge Watchers)

    logger.info("Calculando KPI: Horas de visualización por género...")

    val topGenresDF = topGenre(enrichedDF)

    logger.info("Calculando KPI: Binge Watchers (Maratones)...")

    val bingeWatchersDF = topBinge(logsExtraidoDF)


    // 6. Escribir resultados en Parquet particionado

    logger.info("Guardando resultados en formato Parquet particionado por 'year' y 'country' ...")
    guardar(enrichedDF, outputPath)


    // Despersistir
    logsExtraidoDF.unpersist()
    movieLimpioDF.unpersist()
    enrichedDF.unpersist()

  }


  // 1. Lectura de archivos con excepciones

  def readRawLogsWithTry(path: String)(implicit spark: SparkSession): DataFrame = {
    try {
      spark.read.text(path)
    } catch {
      case e: Exception =>
        logger.error("Error leyendo logs: " + e.getMessage)
        spark.emptyDataFrame
    }
  }

  def readMoviesWithTry(path: String)(implicit spark: SparkSession): DataFrame = {
    try {
      spark.read.option("header", "true").csv(path)    } catch {
      case e: Exception =>
        logger.error("Error leyendo movies: " + e.getMessage)
        spark.emptyDataFrame
    }
  }

  // 2. Limpieza de Logs

  def validLogs(rawDF: DataFrame): DataFrame = {
    val validLogs = rawDF.filter(col("value").startsWith("[INFO]"))
    validLogs
  }

  def logsExtraido (validLogs: DataFrame): DataFrame = {
    val logsExtraido = validLogs
      .withColumn("parts", split(col("value"), Constants.LOG_SEPARATOR))
      .withColumn("inicio", to_timestamp(regexp_replace(col("parts").getItem(0),Constants.INFO_PATTERN, ""), Constants.TIMESTAMP_FORMAT))
      .withColumn("usuario", split(col("parts").getItem(1), ":").getItem(1))
      .withColumn("movie_id", regexp_replace(split(col("parts").getItem(2), ":").getItem(1), Constants.MOVIE_PREFIX, ""))
      .withColumn("duracion", split(col("parts").getItem(3), ":").getItem(1))
      .drop("value", "parts")
    logsExtraido
  }


  // 3. Limpieza de películas

  def movieLimpio (moviesDF: DataFrame): DataFrame = {
    val moviesLimpio = moviesDF.withColumn("subscription_price",
        regexp_replace(col("subscription_price"), Constants.PRICE_SYMBOL, "").cast("Double"))
      .withColumn("genres",
        when(col("genres").isNull || col("genres") === "", "Unknown")
          .otherwise(col("genres")))
    moviesLimpio
  }

  // 4. Enriquecimiento

  def joinLogsWithMovies(logsDF: DataFrame, moviesDF: DataFrame) = {
    val dfUnido = logsDF.join(
      broadcast(moviesDF),
      logsDF("movie_id") === moviesDF("id"),
      "inner"
    ).drop("movie_id")

    dfUnido
  }


  // 5. Cálculo de KPIs

  def topGenre(dfUnido: DataFrame): DataFrame = {
    val topGenre = dfUnido
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(sum("duracion").alias("total_hours"))
    topGenre
  }

  def topBinge (logsDF: DataFrame): DataFrame = {

    val windowSpec = Window.partitionBy("usuario").orderBy("inicio")

    val isBinge = logsDF
      .withColumn("prev_inicio", lag("inicio", 1).over(windowSpec))
      .withColumn("diferencia", (col("inicio").cast(LongType) - col("prev_inicio").cast(LongType)) / 60)
      .withColumn("is_binge", when(col("diferencia") > 0 && col("diferencia") < 20, 1).otherwise(0))

    val topBinge = isBinge.filter(col("is_binge") === 1).groupBy("usuario").agg(count("*").alias("conteo")).orderBy(col("conteo").desc).limit(10)

    topBinge
  }

  // 6. Guardando en Parquet

  def guardar(enrichedDF: DataFrame, outputPath: String): Unit = {
    val dfcompleto = enrichedDF.withColumn("year", year(col("release_date")))
    dfcompleto.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "country")
      .parquet("outputPath")
  }





}