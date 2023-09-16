from df_compare_spark import DF_Compare_spark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("DataFrameComparison").getOrCreate()

# **load files for the example**
# Large Files
df_left = spark.read.csv('demo_data/netflix_titles_orig_short.csv', header=True, inferSchema=True)
df_right = spark.read.csv('demo_data/netflix_titles_mod_short.csv', header=True, inferSchema=True)

exclude_col = ['system_time']
join_col = ['show_id']
comdf = DF_Compare_spark(df_left, df_right, exclude_col, join_col)
# compare_df = comdf.get_differences_by_compare_whole_row()
compare_df = comdf.get_differences_by_deep_compare_join_columns()
compare_df.show(truncate=False)
# compare_df = comdf.get_differences_by_compare_whole_row()
# htdf = df_extract_head_and_tail(compare_df)

# file_name = "test.xlsx"
# compare_df.to_excel(file_name, index=False, engine='openpyxl')