from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, DecimalType

# Create Spark Session 
spark = SparkSession.builder.master("local").appName("azure_upload").getOrCreate()

# Set up access key for Azure blob storage 
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
sc._jsc.hadoopConfiguration().set("fs.azure.account.key.yahoofinancestoragerg.blob.core.windows.net", "")

# Define schema
schema = StructType([
    StructField("date_time", DateType(), True),
    StructField("open", DecimalType(38, 12), True),
    StructField("high",DecimalType(38, 12), True),
    StructField("low", DecimalType(38, 12), True),
    StructField("close", DecimalType(38, 12), True),
    StructField("adj_close", DecimalType(38, 12), True),
    StructField("volume", IntegerType(), True),
])

# Read CSV files from Local System to DataFrames
aapl_df = spark.read.option("header", False).schema(schema).csv("/usr/local/airflow/21.7_Airflow_DAG/data/q/2021-08-14/AAPL.csv")
tsla_df = spark.read.option("header", False).schema(schema).csv("/usr/local/airflow/21.7_Airflow_DAG/data/q/2021-08-14/TSLA.csv")

# Load PARQUET files to Azure blob storage 
aapl_df.write.mode("overwrite").parquet("wasbs://data@yahoofinancestoragerg.blob.core.windows.net/AAPL")
tsla_df.write.mode("overwrite").parquet("wasbs://data@yahoofinancestoragerg.blob.core.windows.net/TSLA")