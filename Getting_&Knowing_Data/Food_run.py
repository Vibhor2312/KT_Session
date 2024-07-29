from pyspark.sql import sparksession
import food as v

spark = sparksession.builder.appname("food_inspection").getorcreate()
df =  spark.read.option("delimeter","t").csv("food_data.tsv",header=true)

food_observation = v.count_observation(df)
print(f"the number of observation are :  {count_observation}")
      

get_columns = v.count_columns(df)
print(f"total columns are:  {count_columns}")

columns_name = v.print_column_names(df)
print(f" names are :  {print_column_names}")


col_105 = v.get_nth_col(df, 105)
print(f"The col num 105 is : {col_105}")



product_19 = v.nth_obs_of_nthRow(df, "product_name",19)
print(f"The product name at 19th row is : {product_19}")



      