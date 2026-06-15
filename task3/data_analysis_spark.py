import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, count, avg, lit, coalesce, sequence, explode

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\Program Files\hadoop"
os.environ["PATH"] = r"C:\Program Files\hadoop\bin;" + os.environ["PATH"]

# Task 3.2: Using the Parquet dataset, write a Spark query that,
# for a given station, computes the average daily delay over the period that the data was collected.
def task3_2(spark_session : SparkSession, station : str):

    result = spark_session.sql(f"""
                               SELECT snapshot_date, station_name, AVG(delay_minutes)
                               FROM (SELECT snapshot_date, station_name, arrival_delay_minutes AS delay_minutes
                                    FROM train_movements
                                    WHERE arrival_delay_minutes IS NOT NULL
                               
                                    UNION ALL
                               
                                    SELECT snapshot_date, station_name, departure_delay_minutes AS delay_minutes
                                    FROM train_movements
                                    WHERE departure_delay_minutes IS NOT NULL
                                ) all_delays
                               WHERE station_name = '{station}'
                               GROUP BY snapshot_date, station_name
                               ORDER BY snapshot_date;
                               """)

    result.show()

# Task 3.3: Return the average number of train departures per station during peak hours (07:00 to 09:00 and 17:00 to 19:00).
def task3_3(df):

    # Count the number of train departures per station and day during peak hours (07:00 to 09:00 and 17:00 to 19:00)
    num_departures_per_station_per_day = (df.filter(col("planned_departure_time").isNotNull())
                                          .filter(((hour(col("planned_departure_time")) >= 7) & (hour(col("planned_departure_time")) < 9)) 
                                                  |((hour(col("planned_departure_time")) >= 17) & (hour(col("planned_departure_time")) < 19)))
                                          .groupBy("station_name", "snapshot_date")
                                          .agg(count("*").alias("num_train_departures")))

    # Create all station-day combinations
    stations_df = df.select("station_name").distinct()
    dates_df = df.select("snapshot_date").distinct()
    station_day_grid = stations_df.crossJoin(dates_df)

    # If a station does not appear on a given day within the time ranges 07:00 to 09:00 and 17:00 to 19:00, 
    # we assume there are no departures and count it as 0
    station_day_counts = (station_day_grid
                          .join(num_departures_per_station_per_day, on=["station_name", "snapshot_date"], how="left")
                          .withColumn("num_train_departures", coalesce(col("num_train_departures"), lit(0))))

    # Average over all days
    result = station_day_counts.groupBy("station_name").agg(avg("num_train_departures").alias("avg_num_train_departures_during_peak")).orderBy("station_name")
    
    result.show()


def main():
    spark = (
        SparkSession.builder
        .appName("DB_ETL_WITH_SPARK")
        .master("local[1]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )

    try:
        # Load parquet dataset and register it as SQL table
        df = spark.read.parquet("output/train_movements")
        df.createOrReplaceTempView("train_movements")

        # Task 3.2
        station = "Berlin Zoologischer Garten"
        print(f"Task 3.2: For a given station, compute the average daily delay over the period that the data was collected. Let's say station=\"{station}\".")
        task3_2(spark, station)

        # Task 3.3
        print("Task 3.3: Return the average number of train departures per station during peak hours (07:00 to 09:00 and 17:00 to 19:00).")
        task3_3(df)

    finally:
        print("Shutting down Spark Session...")
        spark.stop()

if __name__ == "__main__":
    main()