from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Paris Olympics - Silver Transform") \
        .getOrCreate()

    bronze_path = "/workspaces/Paris-Olympics-Data-Engineering-Project/bronze"
    silver_path = "/workspaces/Paris-Olympics-Data-Engineering-Project/silver"
    datasets = ["athletes", "coaches", "events", "nocs"]

    for name in datasets:
        df = spark.read.parquet(f"{bronze_path}/{name}")
        print("Before cleaning:", name, df.count())

        # Basic cleaning: drop duplicate rows, fill nulls with "Unknown"
        df_clean = df.dropDuplicates().na.fill("Unknown")

        print("After cleaning:", name, df_clean.count())

        df_clean.write.mode("overwrite").parquet(f"{silver_path}/{name}")
        print("Silver written for:", name)

    spark.stop()

if __name__ == "__main__":
    main()
