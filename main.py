from pyspark.sql import SparkSession
import dimension
import fact
import extract_sql_to_ibm
import traceback

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("PlexOA-Extraction-Job")
        .enableHiveSupport()
        .getOrCreate()
    )

    print("Running transformations synchronously...")

    # 0. Run Extraction & View Creation
    try:
        print("Starting SQL Server Extraction & View Registration...")
        extract_sql_to_ibm.run(spark)
        print("SQL Server Extraction finished successfully.")
    except Exception as e:
        print(f"Extraction failed: {e}")
        traceback.print_exc()
        spark.stop()
        raise e

    # 1. Run Dimension Transformations
    try:
        print("Starting Dimension Table Transformations...")
        dimension.run(spark)
        print("Dimension Table Transformations finished successfully.")
    except Exception as e:
        print(f"Dimension transformation failed: {e}")
        traceback.print_exc()
        spark.stop()
        raise e

    # 2. Run Fact Transformation
    try:
        print("Starting Fact Table Transformations...")
        fact.run(spark)
        print("Fact Table Transformations finished successfully.")
    except Exception as e:
        print(f"Fact transformation failed: {e}")
        traceback.print_exc()
        spark.stop()
        raise e

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")

