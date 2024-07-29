
import army_filter as army
import pandas as Pd 
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, mean, min, max

spark = SparkSession.builder.appName('SparkBy').getOrCreate()

url = ""
data = pd.read_csv(url, sep="\t")
data.to_csv("chip_data.csv", index=False)
df = spark.read.csv("chip_data.csv", header=True)
df.show()




total_rows = chip.get_data_shape(df)
print(f"The total number of observations are : {total_rows}")


columns = army.get_all_columns(df)
print(f"columns are : {columns}")
      
specific_column = army.get_specific_columns(df)
print(f" specific column is : {specific_column}")    


filter_by_death =army.filter_by_deaths_gt(df)
print(f" death are: {filter_by_death}")


filter_by_death_range = army.filter_by_deaths_range(df)
print(f" people are :{filter_by_death_range}")


filter_by_regiment = army.filter_by_regiment(df)
print(f"regiment are : {filter_by_regiment}")

filter_by_rows = army.filter_by_multiple_rows(df)
print(f" rows are : {filter_by_rows}")


filter_by_multiple_column = army.filter_by_multiple_columns(df)
print(f" columns are : {filter_by_multiple_column}")



dataframe = army.print_dataframe(df)
print(f" data is : {dataframe} ")