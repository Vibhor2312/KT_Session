import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T 
from pyspark.sql.functions import mean


def read_data(url):
  euro12 = pd.read_csv(url, sep = '\t')
  return spark.createDataFrame(data=euro12)



def show_goals(df):                         
  df.select('Goals').show()          


def count_teams(df):
  return df.select('Team').count()


def count_columns(df):
  return len(df.columns)


def show_disipline(df):
  df.select('Team', 'Yellow Cards', 'Red Cards').show()

#show_disipline(euro)

def sort_by_cards(df):
  df.sort("Red Cards", "Yellow Cards").show()





def mean_yellow_cards_per_team(df):
  mean_yellow = df.groupBy("Team","Yellow Cards").count()   
  mean_yellow_per_team = int(mean_yellow.select(mean("Yellow Cards")).collect()[0][0])
  return mean_yellow_per_team


def filter_goals(df):
  return df.filter("Goals > 6").show()


def first_seven(df):
  return df.show(7)


def select_except_last_three(df):
  return df.select(df.columns[35:31:-1]).show()


def filter_teams_starting_with_g(df):
  return df.filter(df["Team"].startswith("G")).show()

