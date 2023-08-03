from pyspark.sql import SparkSession
from pyspark.sql.functions import col

root = r"/datalake/"

def load(spark: SparkSession):
    t0 = spark.read.json(root+r"profiles_history/new_profiles.json")
    t1 = spark.read.json(root+r"profiles_history/profiles_history.json")

    t0_r = t0.select(*(col(x).alias(x + '_df1') for x in t0.columns))
    t1_r = t1.select(*(col(x).alias(x + '_df2') for x in t1.columns))
    return t0_r, t1_r

def run(t0, t1):
    merge = t0.join(t1, col('profile_id_df1') == col('profile_id_df2'), how='right')
    return merge
    
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    
    t0, t1 = load(spark)
    merged = run(t0, t1)
    merged.write.csv(root+r"output2.csv", header=True, mode="overwrite")