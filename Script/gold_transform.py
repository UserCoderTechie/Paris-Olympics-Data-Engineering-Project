from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import os

def main():

    print("\n Starting Gold Layer Transformations...\n")

    spark = SparkSession.builder \
        .appName("Paris Olympics - Gold Transform") \
        .getOrCreate()

    silver_path = "/workspaces/Paris-Olympics-Data-Engineering-Project/silver"
    gold_path = "/workspaces/Paris-Olympics-Data-Engineering-Project/gold"

    # Ensure gold directory exists
    os.makedirs(gold_path, exist_ok=True)

    # ---------------------------------------------
    # Load Silver Tables
    # ---------------------------------------------
    athletes = spark.read.parquet(f"{silver_path}/athletes")
    coaches = spark.read.parquet(f"{silver_path}/coaches")
    events = spark.read.parquet(f"{silver_path}/events")
    nocs = spark.read.parquet(f"{silver_path}/nocs")

    print(" Silver datasets loaded successfully.\n")

    # ---------------------------------------------
    # 1. Athletes per Country
    # ---------------------------------------------
    athletes_per_country = (
        athletes.groupBy("country")
            .agg(count("*").alias("total_athletes"))
            .orderBy(desc("total_athletes"))
    )

    athletes_per_country.write.mode("overwrite") \
        .parquet(f"{gold_path}/athletes_per_country")

    print(" Gold table written: athletes_per_country")

    # ---------------------------------------------
    # 2. Coaches per Country
    # ---------------------------------------------
    coaches_per_country = (
        coaches.groupBy("country")
            .agg(count("*").alias("total_coaches"))
            .orderBy(desc("total_coaches"))
    )

    coaches_per_country.write.mode("overwrite") \
        .parquet(f"{gold_path}/coaches_per_country")

    print(" Gold table written: coaches_per_country")

    # ---------------------------------------------
    # 3. Events per Sport
    # ---------------------------------------------
    events_per_sport = (
        events.groupBy("sport")
            .agg(count("*").alias("event_count"))
            .orderBy(desc("event_count"))
    )

    events_per_sport.write.mode("overwrite") \
        .parquet(f"{gold_path}/events_per_sport")

    print(" Gold table written: events_per_sport")

    # ---------------------------------------------
    # Shutdown Spark
    # ---------------------------------------------
    print("\n Gold Transform Completed Successfully!\n")

    spark.stop()


if __name__ == "__main__":
    main()
