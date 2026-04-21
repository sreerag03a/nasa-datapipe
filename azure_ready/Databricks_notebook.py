spark.conf.set(
    "fs.azure.account.key.sreestorage03.dfs.core.windows.net", 
    "Your_Azure_storage_account_access_key_here"
)

df = spark.read.json("abfss://bronze-layer@sreestorage03.dfs.core.windows.net/neo-data.json")

from pyspark.sql.functions import explode, col
from pyspark.sql.functions import col, explode, map_values, flatten, from_json, to_json
from pyspark.sql.types import MapType, ArrayType, StructType,StringType

my_schema = """map<string, array<struct<
    id:string,
    name:string, 
    absolute_magnitude_h:double, 
    is_potentially_hazardous_asteroid:boolean, 
    estimated_diameter:struct<kilometers:struct<estimated_diameter_max:double>>,
    close_approach_data:array<struct<
        close_approach_date:string, 
        relative_velocity:struct<kilometers_per_second:string>,
        miss_distance:struct<kilometers:string>
    >>
>>>"""

# 2. Process with the new schema
df_flattened = df.withColumn(
    "all_asteroids", 
    flatten(map_values(from_json(to_json(col("near_earth_objects")), my_schema)))
)

# 2. Now explode the flattened list
df_final = df_flattened.select(explode("all_asteroids").alias("asteroid")) \
    .select(
        col("asteroid.id").alias("id"),
        col("asteroid.name").alias("asteroid_name"),
        col("asteroid.absolute_magnitude_h").alias('absolute_magnitude'),
        col("asteroid.estimated_diameter.kilometers.estimated_diameter_max").alias("diameter_km"),
        col("asteroid.close_approach_data")[0].close_approach_date.alias("close_approach_date"),
        col("asteroid.close_approach_data")[0].relative_velocity.kilometers_per_second.cast("double").alias("velocity_kms"),
        col("asteroid.close_approach_data")[0].miss_distance.kilometers.cast("double").alias("miss_distance_km"),
        col("asteroid.is_potentially_hazardous_asteroid").alias("is_hazardous"),
    )

display(df_final)

output_path = "abfss://silver-layer@sreestorage03.dfs.core.windows.net/silver-asteroids"

df_final.write.format("delta") \
    .mode("overwrite").option("mergeSchema", "true") \
    .save(output_path)

print("Data successfully saved to Silver Layer!")

df_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver_asteroids")

from pyspark.sql.functions import avg, count, col
risk_summary = df_final.groupBy("is_hazardous") \
    .agg(
        avg("velocity_kms").alias("avg_velocity"),
        avg("diameter_km").alias("avg_diameter"),
        count("id").alias("total_objects")
    )

daily_trends = df_final.groupBy("close_approach_date") \
    .count() \
    .withColumnRenamed("count", "object_count")

risk_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://gold-layer@sreestorage03.dfs.core.windows.net/risk_summary")
daily_trends.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://gold-layer@sreestorage03.dfs.core.windows.net/daily_trends")

risk_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("risk_summary")
daily_trends.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("daily_trends")



# Drop tables in case of issue : 

# spark.sql("DROP TABLE silver_asteroids")
# spark.sql("DROP TABLE risk_summary")
# spark.sql("DROP TABLE daily_trends")