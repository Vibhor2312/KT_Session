import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.functions import mean
import euro_filter as  s


spark = SparkSession.builder.appName("Test").getOrCreate()

url = 'https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/02_Filtering_%26_Sorting/Euro12/Euro_2012_stats_TEAM.csv'
data = pd.read_csv(url)
data.to_csv("Euro_data.csv",index = False)

df = spark.read.csv("Euro_data.csv", header=True)

goals = s.show_goals(df)
print(f" goals are : {goals}")

teams_count = s.count_teams(df)
print(f" no of team are : {teams_count}")

column_count = s.count_columns(df)
print(f"column are : {column_count}")

disipline = s.show_disipline(df)
print(f" columns are : {disipline}")



sort_by_cards = s.sort_by_cards(df)
print(f"cards are: {sort_by_cards}")


mean_yellow_cards_per_teams = s.mean_yellow_cards_per_team
print(f" mean  is : {mean_yellow_cards_per_teams}")


goals_more_than_6 = s.filter_goals(df)
print(f" teams scored more than 6 goals : {goals_more_than_6}")



show_first_7_rows = s.first_seven(df)
print(f" they are : {show_first_7_rows}")


