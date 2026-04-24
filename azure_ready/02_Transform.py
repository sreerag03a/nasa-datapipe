spark.conf.set(
    "fs.azure.account.key.storageaccount_name_here.dfs.core.windows.net", 
    "Your_Azure_storage_account_access_key_here"
)


df_bronze = spark.table("bronze_asteroids")
display(df_bronze)

from pyspark.sql import functions as F

df_silver = df_bronze.withColumn(
    "avg_diameter_km",
    (F.col("diameter_km_min") + 
     F.col("diameter_km_max")) / 2
)
df_silver = df_silver.drop("diameter_km_min", "diameter_km_max")
display(df_silver)

output_path = "abfss://silver-layer@storageaccount_name_here.dfs.core.windows.net/silver-asteroids"

df_silver.write.format("delta") \
    .mode("overwrite").option("mergeSchema", "true") \
    .save(output_path)
df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver_asteroids")
print("Data successfully saved to Silver Layer!")