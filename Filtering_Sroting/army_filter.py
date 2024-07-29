from pyspark.sql import SparkSession, functions as F

def create_army_dataframe(spark, filepath):
  """
  Reads the fictional army data from a CSV file and creates a PySpark DataFrame with the 'origin' column as the index.

  Args:
      spark: A SparkSession object.
      filepath: The path to the CSV file containing the army data.

  Returns:
      A PySpark DataFrame with the army data.
  """
  army_df = spark.read.csv(filepath, header=True)
  return army_df.withColumn("origin", army_df["origin"]).alias("army").setIndex("origin")


def get_all_columns(df):
  """
  Returns the list of column names in the DataFrame.

  Args:
      df: A PySpark DataFrame.

  Returns:
      A list of column names.
  """
  return df.columns


def get_specific_columns(df, *cols):
  """
  Selects and returns a new DataFrame containing only the specified columns.

  Args:
      df: A PySpark DataFrame.
      *cols: A variable number of column names to select.

  Returns:
      A PySpark DataFrame containing only the specified columns.
  """
  return df.select(*cols)


def filter_by_deaths_gt(df, threshold):
  """
  Filters the DataFrame to include only rows where the 'deaths' column is greater than the specified threshold.

  Args:
      df: A PySpark DataFrame.
      threshold: The minimum number of deaths for filtering.

  Returns:
      A PySpark DataFrame containing rows with deaths greater than the threshold.
  """
  return df.filter(F.col("deaths") > threshold)


def filter_by_deaths_range(df, lower_bound, upper_bound):
  """
  Filters the DataFrame to include rows where the 'deaths' column is within the specified range (inclusive).

  Args:
      df: A PySpark DataFrame.
      lower_bound: The lower bound of the death range.
      upper_bound: The upper bound of the death range.

  Returns:
      A PySpark DataFrame containing rows with deaths within the specified range.
  """
  return df.filter((F.col("deaths") > lower_bound) & (F.col("deaths") <= upper_bound))


def filter_by_regiment(df, regiment_name):
  """
  Filters the DataFrame to include rows where the 'regiment' column matches the specified name.

  Args:
      df: A PySpark DataFrame.
      regiment_name: The name of the regiment to filter by.

  Returns:
      A PySpark DataFrame containing rows where the regiment matches the name.
  """
  return df.filter(F.col("regiment") == regiment_name)


def filter_by_multiple_rows(df, row_list):
  """
  Filters the DataFrame to include only rows with indexes specified in the list.

  Args:
      df: A PySpark DataFrame.
      row_list: A list of row indexes to filter by.

  Returns:
      A PySpark DataFrame containing the filtered rows.
  """
  return df.loc[row_list]


def filter_by_multiple_columns(df, col_list):
  """
  Filters the DataFrame to include only rows where all columns in the list have non-null values.

  Args:
      df: A PySpark DataFrame.
      col_list: A list of column names to filter by.

  Returns:
      A PySpark DataFrame containing rows with non-null values in the specified columns.
  """
  return df.dropna(subset=col_list)





def print_dataframe(df):
  """
  Prints the contents of a PySpark DataFrame.

  Args:
      df: A PySpark DataFrame.
  """
  df.show()


def print_column(df, column_name):
  """
  Prints the values of a specific column in a PySpark DataFrame.

  Args:
      df: A PySpark DataFrame.
      column_name: The name of the column to print.
  """
  df.select(column_name).show()


def get_column_names(df):
  """
  Returns the list of column names in the DataFrame.

  Args:
      df: A PySpark DataFrame.

  Returns:
      A list of column names.
  """
  return df.columns




def get_specific_columns_by_name(df, column_names):
  """
  Selects and returns columns based on their names.

  Args:
      df: A PySpark DataFrame.
      column_names: A list of column names to select.

  Returns:
      A PySpark DataFrame containing the selected columns.
  """
  return df.select(*column_names)


def filter_by_condition(df, condition):
  """
  Filters the DataFrame based on a given condition.

  Args:
      df: A PySpark DataFrame.
      condition: A PySpark Column expression representing the filter condition.

  Returns:
      A PySpark DataFrame containing the filtered rows.
  """
  return df.filter(condition)






