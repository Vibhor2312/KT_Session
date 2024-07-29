from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, mean, min, max

def load_user_data(spark, url, sep, index_col):
  """
  Loads user data from a CSV file into a PySpark DataFrame.

  Args:
      spark (SparkSession): SparkSession object.
      url (str): URL of the CSV file.
      sep (str): Column separator for the CSV file.
      index_col (str): Name of the column to use as the index.

  Returns:
      pyspark.sql.DataFrame: The loaded user data DataFrame.
  """
  return spark.read.csv(url, sep=sep, indexCol=index_col)


def analyze_user_data(user_data_df):
  """
  Analyzes user data and prints various statistics.

  Args:
      user_data_df (pyspark.sql.DataFrame): DataFrame containing user data.
  """
  # Print schema
  user_data_df.printSchema()

  # Analyze occupations
  frequent_occupations = user_data_df.groupBy("occupation").count().orderBy("count", ascending=False)
  frequent_occupations.show()

  total_occupations = user_data_df.count()
  unique_occupations = user_data_df.select(countDistinct("occupation")).collect()[0][0]
  most_frequent_occupation = user_data_df.groupBy("occupation").count().orderBy("count", descending=True).first()

  print("Total occupations:", total_occupations)
  print("Unique occupations:", unique_occupations)
  print("Most frequent occupation:", most_frequent_occupation)

  # Analyze age
  average_age = user_data_df.select(mean(col("age"))).show()
  age_range = user_data_df.select(min(col("age")), max(col("age"))).show()

  print("Average age:", average_age)
  print("Age range:", age_range)



