import pandas as Pd 
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, mean, min, max
import pyspark.sql.functions as F
 

def clean_item_price(df):
  """Cleans the item_price column by extracting the numeric part."""

  return df.withColumn("item_price", F.regexp_extract("item_price", r"(\d+\.\d+)", 1).cast("float"))


def remove_duplicates(df):
  """Removes duplicate rows based on specified columns.

  Args:
    df: The input PySpark DataFrame."""
   
  return df.drop_duplicates(["item_name", "quantity", "choice_description"])


def count_items_above_ten(df):
  """Counts the number of unique items with a price greater than 10."""

  
  return df.filter(df.item_price > 10).select("item_name").distinct().count()





def count_canned_soda_multiple(chipo_filtered):
  # Filter for Canned Soda orders with quantity > 1
  canned_soda_multiple = chipo_filtered.filter((chipo_filtered.item_name == "Canned Soda") & (chipo_filtered.quantity > 1))

  # Count the number of such orders
  count_canned_soda_multiple = canned_soda_multiple.count()
  return count_canned_soda_multiple



def count_veggie_salad_bowls(chipo_filtered):
  # Filter for Veggie Salad Bowl orders
  chipo_salad = chipo_filtered.filter(chipo_filtered.item_name == "Veggie Salad Bowl")
  
  # Count the number of Veggie Salad Bowl orders
  count_salad = chipo_salad.count()
  return count_salad


def most_expensive_item_quantity(chipo_filtered):
  # Find the most expensive item
  most_expensive_item = chipo_filtered.orderBy("item_price", ascending=False).first()

  # Return the quantity of the most expensive item
  return most_expensive_item["quantity"]
