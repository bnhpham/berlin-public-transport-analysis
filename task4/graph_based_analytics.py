import sys
import os
from datetime import datetime
import networkx as nx

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\Program Files\hadoop"
os.environ["PATH"] = r"C:\Program Files\hadoop\bin;" + os.environ["PATH"]


# Subdevide a given stop_id into a trip key and stop index
# trip key is the id of a train that runs from a source to a target station
# stop_index is the intermediate stop on this train ride
# Example: stop_id=-5094610155094601871-2509021141-12
# return trip_key=-5094610155094601871-2509021141 and stop_index=12
def parse_stop_id(stop_id):

    trip_key, stop_index = stop_id.rsplit("-", 1)
    return trip_key, int(stop_index)


# Task 4.1
# Model the berlin train network as a directed graph in NetworkX
def build_graph_from_parquet(df):
    
    # Extract stop_id and station name from spark dataframe
    rows = (df.select("stop_id", "station_name")
            .dropna(subset=["stop_id", "station_name"])
            .collect()
            )

    trips = {}

    # For each train ride, we save all their stops and corresponding station names
    # Example:
    # { "-5094610155094601871-2509021141": [
    #   {"stop_index": 1, "station_name": "Potsdam Hbf (S)"},
    #   {"stop_index": 2, "station_name": "Potsdam-Babelsberg"},
    #   ...
    #   {"stop_index": 29, "station_name": "Ahrensfelde"},
    #   ]
    # }
    for row in rows:
        trip_key, stop_index = parse_stop_id(row["stop_id"])
        trips.setdefault(trip_key, []).append({"stop_index": stop_index, "station_name": row["station_name"]})

    G = nx.DiGraph()

    for trip_key, stops in trips.items():
        # Sort stop list by index
        stops = sorted(stops, key=lambda x: x["stop_index"])

        # Add edges between consecutive stations along the train route
        for current_stop, next_stop in zip(stops, stops[1:]):
            from_station = current_stop["station_name"]
            to_station = next_stop["station_name"]

            if not G.has_edge(from_station, to_station) and from_station != to_station:
                G.add_edge(from_station, to_station)

    return G

# Compute shortest path between a source and target station in a graph G in terms of number of transfers (graph hops)
def shortest_path_graph_hops(G, source, target):
    path = nx.shortest_path(G, source=source, target=target)
    return path


#Task 4.2
def build_graph_with_time_data_from_parquet(df):
    
    # Extract data from spark dataframe
    rows = (df.select("stop_id", "station_name", "planned_arrival_time", "planned_departure_time")
            .dropna(subset=["stop_id", "station_name"])
            .collect()
            )

    trips = {}

    for row in rows:
        trip_key, stop_index = parse_stop_id(row["stop_id"])

        # Save data as dict
        trips.setdefault(trip_key, []).append({
            "stop_index": stop_index,
            "station_name": row["station_name"],
            "arrival_time": row["planned_arrival_time"],
            "departure_time": row["planned_departure_time"]
            })

    G = nx.DiGraph()

    for trip_key, stops in trips.items():
        # Sort stop list by index
        stops = sorted(stops, key=lambda x: x["stop_index"])

        # Add edges between consecutive stations along the train route
        for current_stop, next_stop in zip(stops, stops[1:]):
            from_station = current_stop["station_name"]
            to_station = next_stop["station_name"]

            dp_time = current_stop["departure_time"]
            ar_time = next_stop["arrival_time"]

            if dp_time is None or ar_time is None or from_station == to_station:
                continue

            if not G.has_edge(from_station, to_station):
                G.add_edge(from_station, to_station, connections=[])

            # Compared to build_graph_from_parquet(), we additionally save departure/arrival time and trip_key
            G[from_station][to_station]["connections"].append({
                "dp_time": dp_time,
                "ar_time": ar_time,
                "trip_key": trip_key
                })

    return G


# Task 4.2
# Given a source station, a target station and a departure time,
# Compute the earliest possible arrival time and corresponding path
def earliest_arrival(G, source, target, departure_time):

    earliest = {node: None for node in G.nodes}     # For each train station, we save its earliest arrival time
    earliest[source] = departure_time

    predecessor = {}                                # For each train station, save its predecessor
    all_connections = []

    # Collect all train connections
    for u, v, data in G.edges(data=True):
        for conn in data.get("connections", []):
            all_connections.append((u, v, conn))
    
    # Sort all connections by departure time
    all_connections.sort(key=lambda x: x[2]["dp_time"])

    for u, v, conn in all_connections:
        dp_time = conn["dp_time"]
        ar_time = conn["ar_time"]

        # Check whether train at station u can be catched (departure at u happens later than given departure time)
        if earliest.get(u) is not None and earliest[u] <= dp_time:
            
            # Check whether we can arrive at station v earlier
            if earliest.get(v) is None or ar_time < earliest[v]:
                earliest[v] = ar_time
                predecessor[v] = (u, conn)

    # For the case that target station can not be reached
    if earliest.get(target) is None:
        return None, None

    path = []
    current = target

    # Reconstruct path from target to source
    while current != source:
        path.append(current)
        current = predecessor[current][0]

    path.append(source)
    path.reverse()

    return earliest[target], path


def main():
    spark = ( SparkSession.builder
             .appName("GRAPH_BASED_ANALYTICS")
             .master("local[1]")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.pyspark.python", sys.executable)
             .config("spark.pyspark.driver.python", sys.executable)
             .getOrCreate()
             )

    # Load parquet dataset
    df = spark.read.parquet("output/train_movements")

    # Task 4.1
    source = "Berlin-Spandau"
    target = "Berlin-Wedding"

    print(f"Task 4.1: Given two stations, computes the shortest path between them in terms of the number of transfers (graph hops)," + 
          f"without considering schedules or delays. Let's say source=\"{source}\" and target=\"{target}\".")
    
    G = build_graph_from_parquet(df)
    path = shortest_path_graph_hops(G, source, target)
    num_hops = len(path) - 1

    print("Shortest path: ", path)
    print("Number of graph hops: ", num_hops)

    # Task 4.2
    source = "Berlin Zoologischer Garten"
    target = "Berlin Ostkreuz"
    dp_time = datetime.strptime("2025-09-02 12:00", "%Y-%m-%d %H:%M")

    print(f"Task 4.2: given a source station, a target station, and a departure time, compute the earliest possible arrival time " + 
          f"considering departures, arrivals, and transfers. " + 
          f"Let's say source=\"{source}\", target=\"{target}\" and departure_time=\"{dp_time}\".")

    G_with_t = build_graph_with_time_data_from_parquet(df)
    ar_time, path = earliest_arrival(G_with_t, source, target, dp_time)

    print("Earliest arrival:", ar_time)
    print("Path:", path)

    print("Shutting down Spark Session...")
    spark.stop()


if __name__ == "__main__":
    main()