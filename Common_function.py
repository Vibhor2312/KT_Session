import pyspark.sql.functions as F

def read_data(spark, file_path, file_format="csv", header=True, infer_schema=True, sep=",", **kwargs):
  """
  Reads data into a PySpark DataFrame.

  Args:
      spark: A SparkSession object.
      file_path: The path to the data file.
      file_format: The format of the data file (default: 'csv').
      header: Whether the file has a header row (default: True).
      infer_schema: Whether to infer the schema automatically (default: True).
      sep: The delimiter used in the file (default: ',').
      **kwargs: Additional options for the reader.

  Returns:
      A PySpark DataFrame.
  """

  df = spark.read.format(file_format) \
             .option("header", header) \
             .option("inferSchema", infer_schema) \
             .option("sep", sep) \
             .load(file_path, **kwargs)
  return df




from common_utils import read_data

# Module 1
df1 = read_data(spark, "file1.csv")

# Module 2
df2 = read_data(spark, "file2.parquet", file_format="csv")




###function for handling missing value



from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, isnan, isnull

def handle_missing_values(df: DataFrame, strategy: str = "drop", columns: list = None, fill_value=None):
  """
  Handles missing values in a PySpark DataFrame.

  Args:
      df: The PySpark DataFrame to process.
      strategy: The strategy for handling missing values. Options are "drop", "fill", or "impute".
      columns: A list of columns to apply the strategy to. If None, all columns are considered.
      fill_value: The value to fill missing values with if strategy is "fill".

  Returns:
      The processed PySpark DataFrame.
  """

  if columns is None:
      columns = df.columns

  if strategy == "drop":
      df = df.dropna(subset=columns)
  elif strategy == "fill":
      if fill_value is None:
        raise ValueError("fill_value must be provided when strategy is 'fill'")
      for col_name in columns:
          df = df.withColumn(col_name, when(isnan(col(col_name)) | isnull(col(col_name)), lit(fill_value)).otherwise(col(col_name)))
  elif strategy == "impute":
      # Implement imputation logic here (e.g., using Imputer from pyspark.ml.feature)
      pass
  else:
      raise ValueError("Invalid strategy. Supported strategies are 'drop', 'fill', and 'impute'")

  return df


from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("MissingValueHandling").getOrCreate()

# Load your dataframes
df1 = ...  # Your first DataFrame
df2 = ...  # Your second DataFrame

# Handle missing values in df1
df1_cleaned = handle_missing_values(df1, strategy="fill", columns=["column1", "column2"], fill_value=0)

# Handle missing values in df2
df2_cleaned = handle_missing_values(df2, strategy="drop", columns=["column3", "column4"])

