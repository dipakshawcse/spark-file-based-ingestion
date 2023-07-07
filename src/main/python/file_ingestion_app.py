from pyspark.sql import SparkSession
from pyspark import SparkException
import logging

def read_csv(spark, input_path, header=True, infer_schema=True):
    options = {
        "header": str(header),
        "inferSchema": str(infer_schema)
    }

    return spark.read.format("csv") \
        .options(**options) \
        .load(f"{input_path}/*.csv")

def read_json(spark, input_path, multiline=False):
    options = {
        "multiline": str(multiline)
    }

    return spark.read.format("json") \
        .options(**options) \
        .load(f"{input_path}/*.json")

def read_parquet(spark, input_path):
    return spark.read.format("parquet") \
        .load(f"{input_path}/*.parquet")

def read_avro(spark, input_path):
    return spark.read.format("avro") \
        .load(f"{input_path}/*.avro")

def read_xml(spark, input_path, row_tag):
    options = {
        "rowTag": row_tag
    }

    return spark.read.format("com.databricks.spark.xml") \
        .options(**options) \
        .load(f"{input_path}/*.xml")

def read_excel(spark, input_path, header=True):
    options = {
        "header": str(header)
    }

    return spark.read.format("com.crealytics.spark.excel") \
        .options(**options) \
        .load(f"{input_path}/*.xlsx")

def print_dataframe_info(df, name):
    print(f"\nSchema for {name}:")
    df.printSchema()

    print(f"\nSample records for {name}:")
    df.show(5, truncate=False)

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("File Ingestion App") \
        .getOrCreate()

    try:
        # Set the path to the input file(s)
        input_path = "/path/to/input/files"

        # Read CSV files
        csv_df = read_csv(spark, input_path)

        # Read JSON files
        json_df = read_json(spark, input_path)

        # Read Parquet files
        parquet_df = read_parquet(spark, input_path)

        # Read Avro files
        avro_df = read_avro(spark, input_path)

        # Read XML files
        xml_df = read_xml(spark, input_path, "root")

        # Read Excel files
        excel_df = read_excel(spark, input_path)

        # Print the schema and sample records for each DataFrame
        print_dataframe_info(csv_df, "CSV DataFrame")
        print_dataframe_info(json_df, "JSON DataFrame")
        print_dataframe_info(parquet_df, "Parquet DataFrame")
        print_dataframe_info(avro_df, "Avro DataFrame")
        print_dataframe_info(xml_df, "XML DataFrame")
        print_dataframe_info(excel_df, "Excel DataFrame")

    except SparkException as ex:
        logging.error("Spark error occurred: %s", ex)
    except Exception as ex:
        logging.error("An error occurred: %s", ex)

    finally:
        # Stop the SparkSession
        spark.stop()
