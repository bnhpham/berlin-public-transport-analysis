import psycopg
import datetime
import re

station_lookup = {}


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


# Task 2.1: Given a station name, return its coordinates and identifier
def task2_1(cursor : psycopg.Cursor, station_name : str):

    station_id = get_normalized_id(station_name, station_lookup)

    cursor.execute("""
                   SELECT name, longitude, latitude, eva_number AS identifier FROM dim_station
                   WHERE station_id=%s
                   """, (station_id,))
    
    result = cursor.fetchall()
    print("Result: ", result)


# Task 2.2: Given latitude/longitude, return the name of the closest station.
def task2_2(cursor : psycopg.Cursor, latitude : float, longitude: float):

    # Use L2 norm as distance metric
    # Since we don't want to know the actual distance value, we may drop SQRT to improve performance (order still preserved)
    cursor.execute("""
                   SELECT name, latitude, longitude FROM dim_station
                   ORDER BY SQRT(POWER(latitude - %s, 2) + POWER(longitude - %s, 2))
                   LIMIT 1
                   """, (latitude, longitude))

    result = cursor.fetchall()
    print("Result: ", result)


# Task 2.3: Given a time snapshot (date_hour), return the total number of canceled trains over all 133 stations in Berlin.
def task2_3(cursor : psycopg.Cursor, date_hour: datetime):

    cursor.execute("""
                   SELECT COUNT(*)
                   FROM fact_train_movements f JOIN dim_date d ON f.date_id=d.date_id JOIN dim_time t ON f.time_id=t.time_id
                   WHERE d.date=%s AND EXTRACT(HOUR FROM t.time)=%s AND f.cancelled=TRUE
                   """, (date_hour.date(), date_hour.hour))

    result = cursor.fetchall()
    print("Result: ", result)


# Task 2.4: Given a station name, return the average train delay in that station (in min).
def task2_4(cursor : psycopg.Cursor, station_name : str):
    
    station_id = get_normalized_id(station_name, station_lookup)

    cursor.execute("""
                   SELECT AVG(delay)
                   FROM (
                   SELECT arrival_delay_minutes AS delay
                   FROM fact_train_movements
                   WHERE station_id = %s AND arrival_delay_minutes IS NOT NULL
                   UNION ALL
                   SELECT departure_delay_minutes AS delay
                   FROM fact_train_movements
                   WHERE station_id = %s AND departure_delay_minutes IS NOT NULL
                   ) sub
                   """, (station_id, station_id))
    
    result = cursor.fetchall()
    print("Result: ", result)


def main():
    # Connect to the PostgreSQL database
    connection = psycopg.connect(
        dbname="DBahn-berlin",
        user="postgres",
        password="postgres_password",
        host="localhost"
    )
    cursor = connection.cursor()

    # Station name/id lookup table for different name conventions (e.g. "Alexanderplatz" vs "Berlin Alexanderplatz")
    cursor.execute("SELECT name, station_id FROM dim_station")
    station_lookup = {row[0]: row[1] for row in cursor.fetchall()}

    # Search criteria
    station_name = "Alexanderplatz"                     # Task 2.1
    latitude = 0.0                                      # Task 2.2
    longitude = 0.0                                     # Task 2.2
    date_hour = datetime.datetime(2025, 10, 2, 15)      # Task 2.3
    station_name2 = "Berlin Zoologischer Garten"        # Task 2.4

    # SQL queries
    print(f"Task 2.1: Given a station name, return its coordinates and identifier. Let's say station_name=\"{station_name}\".")
    task2_1(cursor=cursor, station_name=station_name)

    print(f"Task 2.2: Given latitude/longitude, return the name of the closest station. Let's say latitude={station_name} and longitude={longitude}.")
    task2_2(cursor=cursor, latitude=latitude, longitude=longitude)

    print(f"Task 2.3: Given a time snapshot (date_hour), return the total number of canceled trains over all 133 stations in Berlin. Let's say date_hour={date_hour}.")
    task2_3(cursor=cursor, date_hour=date_hour)

    print(f"Task 2.4: Given a station name, return the average train delay in that station (in min). Let's say station_name={station_name2}.")
    task2_4(cursor=cursor, station_name=station_name2)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()