from datetime import datetime
import json
import tarfile
import re
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DateType
from pyspark.sql.functions import col, when, coalesce, lit, max as spark_max

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\Program Files\hadoop"
os.environ["PATH"] = r"C:\Program Files\hadoop\bin;" + os.environ["PATH"]

BATCH_SIZE = 5000

parquet_schema = StructType([
    StructField("snapshot_date", DateType(), True),
    StructField("snapshot_hour", IntegerType(), True),
    StructField("snapshot_minute", IntegerType(), True),
    StructField("stop_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("filter_flags", StringType(), True),
    StructField("trip_type", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("eva_number", IntegerType(), True),
    StructField("trip_category", StringType(), True),
    StructField("train_number", StringType(), True),
    StructField("origin_station", StringType(), True),
    StructField("destination_station", StringType(), True),
    StructField("planned_arrival_time", TimestampType(), True),
    StructField("planned_departure_time", TimestampType(), True),
    StructField("changed_arrival_time", TimestampType(), True),
    StructField("changed_departure_time", TimestampType(), True),
    StructField("arrival_delay_minutes", IntegerType(), True),
    StructField("departure_delay_minutes", IntegerType(), True),
    StructField("cancelled", BooleanType(), True),
    StructField("planned_platform", StringType(), True),
    StructField("changed_platform", StringType(), True)
])


# Extract train data from station_data.json
def extract_station_data():

    with open("datasets/DBahn-berlin/station_data.json") as f:
        file_content = json.load(f)
    
    stations = file_content["result"]
    stations_data = []

    for s in stations:
        stations_data.append((s["number"],
                               s["ifopt"],
                               s["name"],
                               s["mailingAddress"]["city"],
                               s["mailingAddress"]["zipcode"],
                               s["mailingAddress"]["street"],
                               s["category"],
                               s["priceCategory"],
                               s["hasParking"],
                               s["hasBicycleParking"],
                               s["hasLocalPublicTransport"],
                               s["hasPublicFacilities"],
                               s["hasLockerSystem"],
                               s["hasTaxiRank"],
                               s["hasTravelNecessities"],
                               s["hasSteplessAccess"],
                               False if s["hasMobilityService"]=="no" else True,
                               s["hasWiFi"],
                               s["hasTravelCenter"],
                               s["hasRailwayMission"],
                               s["hasDBLounge"],
                               s["hasLostAndFound"],
                               s["hasCarRental"],
                               s["federalState"],
                               s["regionalbereich"]["number"],
                               s["regionalbereich"]["name"],
                               s["regionalbereich"]["shortName"],
                               s["aufgabentraeger"]["shortName"],
                               s["aufgabentraeger"]["name"],
                               s["timeTableOffice"]["email"],
                               s["timeTableOffice"]["name"],
                               s["szentrale"]["number"],
                               s["szentrale"]["publicPhoneNumber"],
                               s["szentrale"]["name"],
                               s["stationManagement"]["number"],
                               s["stationManagement"]["name"],
                               s["evaNumbers"][0]["number"],
                               s["evaNumbers"][0]["geographicCoordinates"]["coordinates"][0],
                               s["evaNumbers"][0]["geographicCoordinates"]["coordinates"][1],
                               s["ril100Identifiers"][0]["rilIdentifier"],
                               s["ril100Identifiers"][0]["hasSteamPermission"],
                               s["ril100Identifiers"][0]["steamPermission"],
                               s["ril100Identifiers"][0]["geographicCoordinates"]["coordinates"][0],
                               s["ril100Identifiers"][0]["geographicCoordinates"]["coordinates"][1],
                               s["ril100Identifiers"][0]["primaryLocationCode"],
                               s["productLine"]["productLine"],
                               s["productLine"]["segment"]
                               ))

    return stations_data


# Returns station_id of given station name by adding/removing the "Berlin-"" or "Berlin " prefix
def get_normalized_id(raw_name, lookup):
    # Try direct match first
    if raw_name in lookup:
        return lookup[raw_name]
    
    # Try removing "Berlin-" or "Berlin "
    alt = re.sub(r'^Berlin([- ]?)', '', raw_name)
    if alt in lookup:
        return lookup[alt]
        
    # Try adding "Berlin-" or "Berlin "
    if f"Berlin-{raw_name}" in lookup:
        return lookup[f"Berlin-{raw_name}"]
    if f"Berlin {raw_name}" in lookup:
        return lookup[f"Berlin {raw_name}"]
    
    # Unknown stations
    return None

# Extract snapshot timestamp from tar member path
# Example: "2509021200/Berlin Zoologischer Garten/timetable.xml" --> datetime(2025, 9, 2, 12, 0)
def parse_snapshot_from_member_name(member_name):

    match = re.search(r"(\d{10})", member_name)

    if match is None:
        raise ValueError(f"No snapshot timestamp found in path: {member_name}")

    raw_snapshot = match.group(1)
    return datetime.strptime(raw_snapshot, "%y%m%d%H%M")

# Accumulating all extracted XML rows in a huge python list causes the python worker to run out of memory and crash.
# Therefore, we write to the parquet in small batches to reduce memory
def write_batch(rows, schema, output_path, partition_cols=None):
    if not rows:
        return

    # Convert Python list to Spark DataFrame
    df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)

    # Make each small batch a single simple local write
    df = df.coalesce(1)

    # Write parquet with optional partitioning
    writer = df.write.mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(output_path)

    # Release memory
    rows.clear()


def create_df_planned_timetables_from_tar(tar_dir, station_lookup):

    timetables_rows = []

    # Open the .tar.gz file
    with tarfile.open(tar_dir, "r:gz") as f:
        # Iterate over all xml files
        members = f.getmembers()
        for member in members:
            
            # Extract data from xml file
            if member.isfile():
                content = f.extractfile(member).read().decode("utf-8")
                root = ET.fromstring(content)

                snapshot_dt = parse_snapshot_from_member_name(member.name)
                snapshot_date = snapshot_dt.date()
                snapshot_hour = snapshot_dt.hour
                snapshot_minute = snapshot_dt.minute

                # Iterate over timetable_stops. Skip if timetable_stops is empty
                timetable_stops = root.findall("s")

                if timetable_stops:
                    # Station information
                    station_name = root.get("station")
                    eva_number = get_normalized_id(station_name, station_lookup)
                    
                    for s in timetable_stops:
                        stop_id = s.get("id")

                        # Train information
                        tl = s.find("tl")
                        filter_flags = tl.get("f")
                        trip_type = tl.get("t")
                        owner = tl.get("o")
                        trip_category = tl.get("c")
                        train_number = tl.get("n")

                        # Facts
                        planned_ar_time = None
                        planned_dp_time = None
                        origin_station = None
                        destination_station = None
                        planned_platform = None
                        cancelled = False

                        # Arrival
                        ar = s.find("ar")
                        if ar is not None:
                            planned_ar_time = datetime.strptime(ar.get("pt"), "%y%m%d%H%M")
                            planned_ar_path = ar.get("ppth")
                            planned_platform = ar.get("pp")
                            origin_station = planned_ar_path.split('|')[0]

                            event_status = ar.get("cs")
                            cancellation_time = ar.get("clt")
                            if (event_status is not None and event_status == "c") or cancellation_time is not None:
                                cancelled = True

                        # Departure
                        dp = s.find("dp")
                        if dp is not None:
                            planned_dp_time = datetime.strptime(dp.get("pt"), "%y%m%d%H%M")
                            planned_dp_path = dp.get("ppth")
                            planned_platform = dp.get("pp") # redundant
                            destination_station = planned_dp_path.split('|')[-1]

                            event_status = dp.get("cs")
                            cancellation_time = dp.get("clt")
                            if (event_status is not None and event_status == "c") or cancellation_time is not None:
                                cancelled = True

                        # Create row for dataframe
                        timetables_rows.append({
                            "snapshot_date": snapshot_date,
                            "snapshot_hour": snapshot_hour,
                            "snapshot_minute": snapshot_minute,
                            "stop_id": stop_id,
                            "station_name": station_name,
                            "filter_flags": filter_flags,
                            "trip_type": trip_type,
                            "owner": owner,
                            "eva_number": eva_number,
                            "trip_category": trip_category,
                            "train_number": train_number,
                            "origin_station": origin_station,
                            "destination_station": destination_station,
                            "planned_arrival_time": planned_ar_time,
                            "planned_departure_time": planned_dp_time,
                            "changed_arrival_time": None,
                            "changed_departure_time": None,
                            "arrival_delay_minutes": None,
                            "departure_delay_minutes": None,
                            "cancelled": cancelled,
                            "planned_platform": planned_platform,
                            "changed_platform": None,
                        })

                        if len(timetables_rows) >= BATCH_SIZE:
                            write_batch(timetables_rows, parquet_schema, "output/planned_raw", None)

    write_batch(timetables_rows, parquet_schema, "output/planned_raw", None)

    return spark.read.parquet("output/planned_raw")
                            

def create_df_timetable_changes_from_tar(tar_dir):

    timetables_changes_rows = []

    # Open the .tar.gz file
    with tarfile.open(tar_dir, "r:gz") as f:
        # Iterate over all xml files
        members = f.getmembers()
        for member in members:
            
            # Extract data from xml file
            if member.isfile():
                content = f.extractfile(member).read().decode("utf-8")
                root = ET.fromstring(content)

                # Iterate over timetable_stops. Skip if timetable_stops is empty
                timetable_stops = root.findall("s")

                if timetable_stops:
                    for s in timetable_stops:
                        stop_id = s.get("id")
                        
                        changed_ar_time = None
                        changed_dp_time = None
                        changed_platform = None
                        cancelled = False
                        unmatched = False

                        # Changed arrival
                        ar = s.find("ar")
                        if ar is not None:
                            event_status = ar.get("cs")
                            cancellation_time = ar.get("clt")
                            if (event_status is not None and event_status == "c") or cancellation_time is not None:
                                cancelled = True

                            # Changed arrival time
                            ct = ar.get("ct")
                            if ct is not None:
                                changed_ar_time = datetime.strptime(ct, "%y%m%d%H%M")
                            
                            # Changed platform
                            changed_platform = ar.get("cp")

                        # Changed departure
                        dp = s.find("dp")
                        if dp is not None:
                            event_status = dp.get("cs")
                            cancellation_time = dp.get("clt")
                            if (event_status is not None and event_status == "c") or cancellation_time is not None:
                                cancelled = True

                            # Changed departure time
                            ct = dp.get("ct")
                            if ct is not None:
                                changed_dp_time = datetime.strptime(ct, "%y%m%d%H%M")
                                
                                # Changed platform
                                changed_platform = dp.get("cp")

                        # Create row for dataframe
                        timetables_changes_rows.append({
                            "stop_id": stop_id,
                            "changed_arrival_time": changed_ar_time,
                            "changed_departure_time": changed_dp_time,
                            "cancelled": cancelled,
                            "changed_platform": changed_platform,
                        })

                        if len(timetables_changes_rows) >= BATCH_SIZE:
                            write_batch(timetables_changes_rows, None, "output/changes_raw")

    write_batch(timetables_changes_rows, None, "output/changes_raw")

    return spark.read.parquet("output/changes_raw")


def load_data_into_spark(tar_dir_planned, tar_dir_changes, station_lookup):

    # Extract planned timetables and timetable changes as dataframes
    planned_df = create_df_planned_timetables_from_tar(tar_dir_planned, station_lookup)
    changes_df = create_df_timetable_changes_from_tar(tar_dir_changes)

    # changes_df might have multiple rows for the same stop_id as timetable changes are collected every 15 minutes
    # That causes the join to duplicate planned rows
    # We use max()/spark_max() since timestamps increase over time and we want the latest known prediction
    changes_df = changes_df.groupBy("stop_id").agg(
        spark_max("changed_arrival_time").alias("changed_arrival_time"),
        spark_max("changed_departure_time").alias("changed_departure_time"),
        spark_max("cancelled").alias("cancelled"),
        spark_max("changed_platform").alias("changed_platform")
    )

    p = planned_df.alias("p")
    c = changes_df.alias("c")

    # Merge both dataframes
    merged_df = (
        p.join(c, on="stop_id", how="left")
        .select(
            col("stop_id"),
            
            # all planned/base columns
            col("p.snapshot_date"),
            col("p.snapshot_hour"),
            col("p.snapshot_minute"),
            col("p.station_name"),
            col("p.filter_flags"),
            col("p.trip_type"),
            col("p.owner"),
            col("p.eva_number"),
            col("p.trip_category"),
            col("p.train_number"),
            col("p.origin_station"),
            col("p.destination_station"),
            col("p.planned_arrival_time"),
            col("p.planned_departure_time"),
            col("p.planned_platform"),

            # overwrite/update fields from changes
            coalesce(col("c.changed_arrival_time"), col("p.changed_arrival_time")).alias("changed_arrival_time"),
            coalesce(col("c.changed_departure_time"), col("p.changed_departure_time")).alias("changed_departure_time"),
            coalesce(col("c.changed_platform"), col("p.changed_platform")).alias("changed_platform"),

            # if either planned or changed says cancelled = true
            (coalesce(col("p.cancelled"), lit(False)) | coalesce(col("c.cancelled"), lit(False))).alias("cancelled"))
        )

    # Compute arrival delays
    merged_df = merged_df.withColumn(
        "arrival_delay_minutes",
        when(
            col("planned_arrival_time").isNotNull() &
            col("changed_arrival_time").isNotNull(),
            ((col("changed_arrival_time").cast("long") -
            col("planned_arrival_time").cast("long")) / 60).cast("int")
            )
        )

    # Compute departure delays
    merged_df = merged_df.withColumn(
        "departure_delay_minutes",
        when(
            col("planned_departure_time").isNotNull() &
            col("changed_departure_time").isNotNull(),
            ((col("changed_departure_time").cast("long") -
            col("planned_departure_time").cast("long")) / 60).cast("int")
            )
        )

    merged_df.write.mode("append").partitionBy("snapshot_date", "snapshot_hour", "snapshot_minute").parquet("output/train_movements")

if __name__ == "__main__":

    spark = (SparkSession.builder
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
        # Extract station data
        stations_data = extract_station_data()
        station_lookup = {row[2]: row[36] for row in stations_data} # row[2] = name, row[36] = eva_number

        weeks = ["250902_250909", "250909_250916", "250916_250923", "250923_250930", "250930_251007", "251007_251014", "251014_251021"]

        # Iterate through all timetable and timetable change files and store the data as a parquet dataset
        for w in weeks:    
            print(f"Extracting timetables data for week \"{w}\"...")
            tar_dir_planned = "datasets/DBahn-berlin/timetables/" + w + ".tar.gz"
            tar_dir_changes = "datasets/DBahn-berlin/timetable_changes/" + w + ".tar.gz"
            load_data_into_spark(tar_dir_planned, tar_dir_changes, station_lookup)
    finally:
        print("Shutting down Spark Session...")
        spark.stop()