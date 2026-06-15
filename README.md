# Data Integration and Large-scale Analysis WiSe 2025 Exercise

This exercise implements ETL pipelines for data integration and analysis of real-world train data collected via the Deutsche Bahn (DB) API Marketplace between September 2 and October 15, 2025. It covers a star schema design, a PostgreSQL ingestion pipeline as well as a distributed large-scale ETL using Apache Spark with Parquet output. Analytical queries were developed and executed on both systems to extract insights. Finally, the Berlin S-Bahn network is modeled as a directed graph in NetworkX to compute shortest paths and earliest arrival times between stations.

## Data Source
The dataset is not included in this repository as it was provided as part of the course. However, it can be accessed via the [Deutsche Bahn API Marketplace](https://developers.deutschebahn.com/db-api-marketplace/apis/).
The data consists of three components:
* **Stations**: Metadata for all 133 S-Bahn stations in Berlin (`.json`).
* **Timetables**: Planned train movements collected hourly (`.xml`).
* **Timetable Changes**: Disruptions (delays, cancellations) collected every 15 minutes (`.xml`).

The collected data is contained into weekly `.tar.gz` archives representing hourly timetables and 15-minutes sampled timetable_changes:
* `timetables/`: Hourly snapshots (`YYMMDDHHMM`, e.g. `2509021200`).
* `timetable_changes/`: 15-minute snapshots (`YYMMDDHHMM`, minutes ∈ {00,15,30,45}).

### Weekly containers
The files are contained into fixed 7-day windows `.tar.gz` files anchored at the earliest snapshot date. Each file name `YYMMDD_YYMMDD.tar.gz` represents the start and end of the week with the second date exclusive. Each `.tar.gz` the information for the timetables that fall in that 7-day window.

### On-disk layout (archives)
```
.
├─ timetables/
│  ├─ 250902_250909.tar.gz
│  ├─ 250909_250916.tar.gz
│  └─ …
└─ timetable_changes/
   ├─ 250902_250909.tar.gz
   ├─ 250909_250916.tar.gz
   └─ …
```

### Inside an archive (example)
```
250902_250909.tar.gz
├─ 2509021200/   # 2025-09-02 12:00
├─ 2509021300/   # 2025-09-02 13:00
└─ …             # up to, but not including, 2025-09-09 00:00
```

For `timetable_changes/`, folders are every 15 minutes (e.g., `2509021215/`, `2509021230/`, `2509021245/`).

## Task 1 & 2: Schema Design, Data Ingestion & Analysis in PostgreSQL
* A star schema designed to represent train movements:
    * Fact Table: `train_movements` (planned arrival and departure times, delays, cancellations, platforms)
    * 4 Dimension Tables: stations, trains, date, time
* Ingestion pipeline that parses `.json` and `.xml` files and loads the data into the schema in PostgreSQL.
* Four SQL queries were implemented on the ingested dataset:
    * 2.1 - Given a station name, return its coordinates and identifier.
    * 2.2 - Given latitude/longitude, return the name of the closest station.
    * 2.3 - Given a time snapshot, return the total number of canceled trains over all 133 stations in Berlin.
    * 2.4 - Given a station name, return the average train delay in that station.

## Task 3: Large-Scale ETL and Analysis in Spark
* Implementation of an end-to-end Apache Spark ETL pipeline to process the raw `.xml` files, extract relevant fields and store the result as a Parquet dataset partitioned by time snapshots.
* Two analytical queries were designed on the Parquet dataset:
    * 3.2 - Given a station name, return the average daily delay over the period that the data was collected.
    * 3.3 - Return the average number of train departures per station during peak hours (07:00 to 09:00 and 17:00 to 19:00).

## Task 4: Graph-Based Analytics
* Using the Spark Parquet output, the Berlin S-Bahn network is modeled as a directed graph in NetworkX where stations are nodes and direct train connections are edges.
* Two graph algorithms were applied:
    * 4.1 - Shortest Path: Computes the shortest path between two given stations in terms of the number of transfers (graph hops), without considering schedules or delays.
    * 4.2 - Earliest Arrival Time: Given a source, target, and departure time, computes the earliest possible arrival time considering departures, arrivals, and transfers. It is assumed that all transfers are possible whenever arrival and departure times at a station are consistent.

## Requirements
* Python 3.10.11
* Java 17
* PostgreSQL 18.3
* Hadoop 3.3.5
* Pyspark 3.5.1 (Python API for Apache Spark)

