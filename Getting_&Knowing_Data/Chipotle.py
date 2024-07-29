from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
import pyspark.sql.types as T 


def create_spark_session():
  """Creates a SparkSession object."""
  spark = SparkSession.builder.appName("ChipotleAnalysis").getOrCreate()
  return spark

def load_data(spark, url):
  """Loads data from a URL into a PySpark DataFrame."""
  df = spark.read.csv(url, sep='\t', header=True)
  return df

def get_data_shape(df):
  """Returns the number of observations and columns in the DataFrame."""
  return df.count(), df.schema.fields.__len__()

def get_column_names(df):
  """Returns a list of column names in the DataFrame."""
  return df.columns

def get_index_type(df):
  """Returns the type of index used in the DataFrame."""
  return df.rdd.getNumPartitions()  # Number of partitions represents distribution

def find_most_ordered_item(df):
  """Finds the most ordered item based on quantity."""
  order_counts = df.groupBy('item_name').agg(F.sum('quantity').alias('total_orders'))
  return order_counts.orderBy(F.desc('total_orders')).first()

def find_most_ordered_choice(df):
  """Finds the most ordered choice_description based on quantity."""
  choice_counts = df.groupBy('choice_description').agg(F.sum('quantity').alias('total_orders'))
  return choice_counts.orderBy(F.desc('total_orders')).first()

def get_total_items_ordered(df):
  """Calculates the total number of items ordered."""
  return df.select(F.sum('quantity')).first()[0]  # Access the first element of the result

def convert_item_price_to_float(df):
  """Defines a UDF to convert item_price from string to float."""
  def dollarizer(price_str):
    return float(price_str[1:-1])
  udf_dollarizer = F.udf(dollarizer, F.FloatType())
  return df.withColumn('item_price_float', udf_dollarizer(df['item_price']))

#def calculate_revenue(df):
  """Calculates the total revenue based on quantity and item_price."""
  df = convert_item_price_to_float(df)  # Ensure item_price is float
  return df.select(F.sum(F.col('quantity') * F.col('item_price_float'))).first()[0]


# functin to calculate revenue where revenue = quantity * item_price
def get_revenue(df):
  df = df.withColumn('revenue', df['quantity'] * df['item_price'])
  revenue = df.select(F.sum('revenue')).collect()[0][0]
  return revenue




def count_orders(df):
  """Counts the number of distinct orders."""
  return df.select('order_id').distinct().count()

def calculate_average_revenue_per_order(df):
  """Calculates the average revenue per order."""
  df = convert_item_price_to_float(df)  # Ensure item_price is float
  df = df.withColumn('revenue', F.col('quantity') * F.col('item_price_float'))
  order_averages = df.groupBy('order_id').agg(F.avg('revenue').alias('avg_revenue'))
  return order_averages.select(F.avg('avg_revenue')).first()[0]

def count_unique_items(df):
  """Counts the number of distinct items sold."""
  return df.select('item_name').distinct().count()

