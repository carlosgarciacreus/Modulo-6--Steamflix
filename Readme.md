# StreamFlix Analytics - Módulo 6

## Descripción
Job ETL de Spark que procesa logs de visualización de StreamFlix, los cruza con el catálogo de películas y genera un fichero Parquet con los datos enriquecidos.

## Requisitos
- Java 11
- Scala 2.12.15
- SBT 1.12.11
- Apache Spark 3.3.0 instalado y configurado en el PATH

## Compilación
Desde la carpeta raíz del proyecto ejecutar:

sbt package

Esto genera el JAR en target/scala-2.12/streamflixanalytics_2.12-0.1.jar

## Ejecución con spark-submit

spark-submit --class Main --master local[*] target/scala-2.12/streamflixanalytics_2.12-0.1.jar <inputPath> <outputPath>

## Ejemplo

spark-submit --class Main --master local[*] target/scala-2.12/streamflixanalytics_2.12-0.1.jar src/main/resources src/main/resources/output

## Argumentos
- inputPath: carpeta donde están los ficheros server_logs.txt y movies_metadata.csv
- outputPath: carpeta donde se guardarán los resultados en formato Parquet

## Resultado
El job genera un fichero Parquet en outputPath con los logs enriquecidos con el catálogo de películas, listo para ser consumido por el equipo de Data Science.