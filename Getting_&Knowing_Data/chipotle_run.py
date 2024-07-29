import  Chipotle as chip
import pandas as Pd 
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, mean, min, max



from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkBy').getOrCreate()



url = "https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv"
data = pd.read_csv(url, sep="\t")
data.to_csv("chip_data.csv", index=False)
df = spark.read.csv("chip_data.csv", header=True)
df.show()



total_rows = chip.get_data_shape(df)
print(f"The total number of observations are : {total_rows}")


total_column = chip.get_column_names(df)
print(f"The total number of columns are : {total_column}")


column_name = chip.get_column_names(df)
print(f"The column name are: {column_name}")


most_ordered_item = chip.find_most_ordered_item(df)
print(f"The most ordered item are : {most_ordered_item}")


most_ordered_choice=chip.find_most_ordered_choice(df)
print(f"The most ordered choice are : {most_ordered_choice}")


total_number_of_items_ordered=chip.get_total_items_ordered(df)
print(f"The items ordered : {total_number_of_items_ordered}")

item_to_float = chip.convert_item_price_to_float(df)
print(f"The dtype of {df.dtypes[4][0]} is {df.dtypes[4][1]}")


total_revenue = chip.get_revenue(df)
print(f"The total Revenue is : ${round(total_revenue,2)}")

count_order = chip.count_orders(df)
print(f"The counts is: {count_order}")


avg_rev_per_order= chip.calculate_average_revenue_per_order(df)
print(f"The avg rev per order are : {avg_rev_per_order}")


count_unique_item = chip.count_unique_items(df)
print(f"The most ordered choice are : {count_order}")
