import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkException
import org.slf4j.LoggerFactory

object FileIngestionApp {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("File Ingestion App")
      .getOrCreate()

    try {
      // Set the path to the input file(s)
      val inputPath = "/path/to/input/files"

      // Read CSV files
      val csvDF = readCSV(spark, inputPath, header = true, inferSchema = true)

      // Read JSON files
      val jsonDF = readJSON(spark, inputPath, multiline = true)

      // Read Parquet files
      val parquetDF = readParquet(spark, inputPath)

      // Read Avro files
      val avroDF = readAvro(spark, inputPath)

      // Read XML files
      val xmlDF = readXML(spark, inputPath, rowTag = "root")

      // Read Excel files
      val excelDF = readExcel(spark, inputPath, header = true)

      // Perform transformations or further processing on the DataFrames
      // ...

      // Print the schema and sample records for each DataFrame
      printDataFrameInfo(csvDF, "CSV DataFrame")
      printDataFrameInfo(jsonDF, "JSON DataFrame")
      printDataFrameInfo(parquetDF, "Parquet DataFrame")
      printDataFrameInfo(avroDF, "Avro DataFrame")
      printDataFrameInfo(xmlDF, "XML DataFrame")
      printDataFrameInfo(excelDF, "Excel DataFrame")

    } catch {
      case ex: SparkException =>
        logger.error("Spark error occurred:", ex)
      case ex: Exception =>
        logger.error("An error occurred:", ex)
    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }

  def readCSV(spark: SparkSession, inputPath: String, header: Boolean, inferSchema: Boolean): DataFrame = {
    val options = Map(
      "header" -> header.toString,
      "inferSchema" -> inferSchema.toString
    )

    spark.read.format("csv")
      .options(options)
      .load(s"$inputPath/*.csv")
  }

  def readJSON(spark: SparkSession, inputPath: String, multiline: Boolean): DataFrame = {
    val options = Map(
      "multiline" -> multiline.toString
    )

    spark.read.format("json")
      .options(options)
      .load(s"$inputPath/*.json")
  }

  def readParquet(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read.format("parquet")
      .load(s"$inputPath/*.parquet")
  }

  def readAvro(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read.format("avro")
      .load(s"$inputPath/*.avro")
  }

  def readXML(spark: SparkSession, inputPath: String, rowTag: String): DataFrame = {
    val options = Map(
      "rowTag" -> rowTag
    )

    spark.read.format("com.databricks.spark.xml")
      .options(options)
      .load(s"$inputPath/*.xml")
  }

  def readExcel(spark: SparkSession, inputPath: String, header: Boolean): DataFrame = {
    val options = Map(
      "header" -> header.toString
    )

    spark.read.format("com.crealytics.spark.excel")
      .options(options)
      .load(s"$inputPath/*.xlsx")
  }

  def printDataFrameInfo(df: DataFrame, name: String): Unit = {
    println(s"\nSchema for $name:")
    df.printSchema()

    println(s"\nSample records for $name:")
    df.show(5, truncate = false)
  }
}