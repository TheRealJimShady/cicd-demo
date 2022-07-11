from pyspark.sql.functions import col

def sum_groups(spark, table_name): 
    return spark.range(0, 1000).toDF("value").withColumn("group", col("value") % 10).groupBy("group").sum("value").withColumnRenamed("sum(value)", "sum").write.mode("overwrite").format("delta").saveAsTable(table_name)