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


def select_specific_rows_and_columns(df, start_row, end_row, start_col, end_col):
  """
  Selects a sub-DataFrame containing rows from a specific range and columns from another range (inclusive).

  Args:
      df: A PySpark DataFrame.
      start_row: The starting row index (inclusive).
      end_row: The ending row index (inclusive).
      start_col: The starting column index (inclusive).
      end_col: The ending column index (inclusive).

  Returns:
      A PySpark DataFrame containing the specified rows and columns.
  """
  # Note: This function might not be directly applicable to PySpark DataFrames due to their distributed nature.
  # Consider using filtering and column selection instead.


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


def get_specific_rows(df, row_indices):
  """
  Selects and returns rows based on their index.

  Args:
      df: A PySpark DataFrame.
      row_indices: A list of row indices to select.

  Returns:
      A PySpark DataFrame containing the selected rows.
  """
  # Note: This function might not be directly applicable to PySpark DataFrames due to their distributed nature.
  # Consider using filtering or other methods to achieve similar results.


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


def select_rows_by_index(df, start_index, end_index):
  """
  Selects rows from a DataFrame based on their index.

  Args:
      df: A PySpark DataFrame.
      start_index: The starting index (inclusive).
      end_index: The ending index (inclusive).

  Returns:
      A PySpark DataFrame containing the selected rows.
  """
  # Note: This function might not be directly applicable to PySpark DataFrames due to their distributed nature.
  # Consider using filtering or other methods to achieve similar results.


def select_columns_by_index(df, start_index, end_index):
  """
  Selects columns from a DataFrame based on their index.

  Args:
      df: A PySpark DataFrame.
      start_index: The starting column index (inclusive).
      end_index: The ending column index (inclusive).

  Returns:
      A PySpark DataFrame containing the selected columns.
  """
  # Note: This function might not be directly applicable to PySpark DataFrames due to their distributed nature.
  # Consider using column names or filtering to achieve similar results.

