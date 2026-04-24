spark.conf.set(
    "fs.azure.account.key.storageaccount_name_here.dfs.core.windows.net", 
    "Your_Azure_storage_account_access_key_here"
)

df_silver = spark.table("silver_asteroids")

from pyspark.sql.functions import avg, count, col
risk_summary = df_silver.groupBy("is_hazardous") \
    .agg(
        avg("velocity_kms").alias("avg_velocity"),
        avg("avg_diameter_km").alias("avg_diameter"),
        count("id").alias("total_objects")
    )

daily_trends = df_silver.groupBy("close_approach_date") \
    .count() \
    .withColumnRenamed("count", "object_count")

risk_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://gold-layer@storageaccount_name_here.dfs.core.windows.net/risk_summary")
daily_trends.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://gold-layer@storageaccount_name_here.dfs.core.windows.net/daily_trends")

risk_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("risk_summary")
daily_trends.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("daily_trends")