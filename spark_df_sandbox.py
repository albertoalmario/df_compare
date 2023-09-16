from df_compare_spark import DF_Compare_spark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("DataFrameComparison").getOrCreate()

#**load files for the example**
df_left = spark.read.csv('demo_data/netflix_titles_orig_short.csv', header=True, inferSchema=True)
df_right = spark.read.csv('demo_data/netflix_titles_mod_short.csv', header=True, inferSchema=True)

df_left.show()

print("Add column with fix value")
df = df_left.withColumn('New_Column', F.lit('Fix Value Example'))
df.show()

print("Filter a value")
df = df_left.filter(F.col("type") == "TV Show")
df.show()

print("Like Filter case sensitive")
df = df_left.filter(F.expr("type LIKE '%show%'"))
df.show()

print("iLike Filter no case sensitive")
df = df_left.filter(F.expr("type ILIKE '%show%'"))
df.show()

print("in Filter")
df = df_left.filter(F.col("title").isin([3,7,23]))
df.show()

print("inner join and get only the left columns")
df_inn_join = df_left.join(df_right, df_left.show_id == df_right.show_id, how='inner')
df_inn_join.show()

print("left join using where")
df_lf_join = df_left.join(df_right, df_left.show_id == df_right.show_id, how='left').where(df_right.show_id.isNull())
df_lf_join.show()

print("leftanti join show the row of the left side that doesn't mach with right side and will show only the columns of the left df (doesn't exist rightanti)")
df_lf_join = df_left.join(df_right, df_left.show_id == df_right.show_id, how='leftanti')
df_lf_join.show()

print("right join using where")
df_rg_join = df_left.join(df_right, df_left.show_id == df_right.show_id, how='right').where(df_left.show_id.isNull())
df_rg_join.show()

print("join and select only the columns of the left df (same that leftanti)")
df_join = (df_left.alias("l")
                .join(df_right.alias("r"), F.col("l.show_id") == F.col("r.show_id"), how="right")
                .select(*["l." + col for col  in df_left.columns]))
df_join.show()

print("union rows that don't matched")
df_lf_join = df_left.join(df_right, df_left.show_id == df_right.show_id, how='leftanti').withColumn('join_status', F.lit('left_only'))
df_rg_join = df_right.join(df_left, df_right.show_id == df_left.show_id, how='leftanti').withColumn('join_status', F.lit('right_only'))
df_lf_rg_union = df_lf_join.union(df_rg_join)
df_lf_rg_union.show()

print("sort columns alphabetical")
df_lf_join = df_lf_join.select(*(sorted(df_lf_join.columns)))
df_rg_join = df_rg_join.select(*(sorted(df_rg_join.columns)))
df_lf_rg_union = df_lf_join.union(df_rg_join)
df_lf_rg_union.show()

