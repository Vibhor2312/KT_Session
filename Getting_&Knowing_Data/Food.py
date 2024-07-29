from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

def create_spark_session():
  """Creates a SparkSession object."""
  spark = SparkSession.builder.appName("FoodFactsAnalysis").getOrCreate()
  return spark

def load_data(spark, filepath):
  """Loads the food data from a TSV file into a PySpark DataFrame."""
  df = spark.read.csv(filepath, sep="\t", header=True)
  return df



  
def count_observations(df):
  """Counts the number of observations in a DataFrame."""
  return df.count()

def count_columns(df):
  """Counts the number of columns in a DataFrame."""
  return len(df.columns)

def print_column_names(df):
  """Prints the column names of a DataFrame."""
  df.show(truncate=False)

def print_data_types(df):
  """Prints the data types of columns in a DataFrame."""
  df.printSchema()

def get_nth_col(df,n):
  cols = df.columns
  return cols[n-1]

def nth_obs_ofnth_row(df,col_name,obs_name):
  return df.select(col_name).head(obs_num)[obs_num-1][0]  







