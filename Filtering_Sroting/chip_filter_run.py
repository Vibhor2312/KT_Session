import chip_filter as v
import pandas as Pd 
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, mean, min, max

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkBy').getOrCreate()

url = "https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/02_Filtering_%26_Sorting/Euro12/Euro_2012_stats_TEAM.csv"
data = pd.read_csv(url, sep="\t")
data.to_csv("chip_data.csv", index=False)
df = spark.read.csv("chip_data.csv", header=True)
df.show()


clean_item_price = v.clean_item_price(df)
print(f"converted : {clean_item_price}")

remove_duplicates =v.remove_duplicates(df)
print(f" removed : {remove_duplicates}")

item_above_10 = v.count_items_above_ten(df)
print(f" item are : {item_above_10}")

canned_soda_count = v.count_canned_soda_multiple(df)
print(f" they are : {canned_soda_count}")

veggie_salad_count =  v.count_veggie_salad_bowls(df)
print(f"count is : {veggie_salad_count}")

most_expensive_item = v.most_expensive_item_quantity(df)
print(f"quantity is : {most_expensive_item}")




