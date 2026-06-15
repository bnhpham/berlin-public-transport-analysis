import psycopg
import json
import tarfile
import xml.etree.ElementTree as ET
from datetime import datetime
import re

station_lookup = {}
date_lookup = {}
time_lookup = {}
train_lookup = {}


# Read, extract and load the train data from station_data.json
def extract_station_data(connection, cursor):

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

    # Load the data into the database
    cursor.executemany("""
                       INSERT INTO dim_station (number, ifopt, name, city, zipcode, street, category, priceCategory, hasParking, hasBicycleParking, hasLocalPublicTransport,
                       hasPublicFacilities, hasLockerSystem, hasTaxiRank, hasTravelNecessities, hasSteplessAccess, hasMobilityService, hasWiFi,
                       hasTravelCenter, hasRailwayMission, hasDBLounge, hasLostAndFound, hasCarRental, federalState, regionalbereich_number,
                       regionalbereich_name, regionalbereich_shortName, aufgabentraeger_shortName, aufgabentraeger_name, timeTableOffice_email,
                       timeTableOffice_name, szentrale_number, szentrale_publicPhoneNumber, szentrale_name, stationManagement_number, stationManagement_name, 
                       eva_number, longitude, latitude, rilIdentifier, rilIdentifier_hasSteamPermission, rilIdentifier_steamPermission, rilIdentifier_longitude,
                       rilIdentifier_latitude, rilIdentifier_primaryLocationCode, productLine, segment)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       """, stations_data)
    connection.commit()


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


# Calculate the difference between two datetime objects in minutes
def get_minute_diff(time_a, time_b):
    # Assume time_b is the later timestamp
    delta = time_b - time_a
    
    # Convert the total duration into minutes
    minutes = delta.total_seconds() / 60
    
    return int(minutes)


def extract_timetables_from_tar(connection, cursor, tar_dir):

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
                    # Get station_id from station table
                    station_name = root.get("station")
                    
                    station_id = get_normalized_id(station_name, station_lookup)

                    for s in timetable_stops:
                        stop_id = s.get("id")

                        # Extract date and time from stop_id
                        if stop_id[0] == "-":
                            stop_id = stop_id[1:]
                        raw_timestamp = stop_id.split('-')[1]
                        datetime_obj = datetime.strptime(raw_timestamp, "%y%m%d%H%M")
                        date_str = datetime_obj.strftime("%Y-%m-%d")
                        time_str = datetime_obj.strftime("%H:%M:%S")

                        # Load date & time into database
                        if date_str in date_lookup:
                            date_id = date_lookup[date_str]
                        else:
                            cursor.execute("""
                                       INSERT INTO dim_date (date)
                                       VALUES (%s)
                                       RETURNING date_id
                                       """, (date_str,))
                            date_id = cursor.fetchone()[0]
                            date_lookup[date_str] = date_id

                        if time_str in time_lookup:
                            time_id = time_lookup[time_str]
                        else:
                            cursor.execute("""
                                       INSERT INTO dim_time (time)
                                       VALUES (%s)
                                       RETURNING time_id
                                       """, (time_str,))
                            time_id = cursor.fetchone()[0]
                            time_lookup[time_str] = time_id

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
                            planned_platform = dp.get("pp")
                            destination_station = planned_dp_path.split('|')[-1]

                            event_status = dp.get("cs")
                            cancellation_time = dp.get("clt")
                            if (event_status is not None and event_status == "c") or cancellation_time is not None:
                                cancelled = True

                        # Load train data into train table
                        train_key = (filter_flags, trip_type, owner, trip_category, train_number, origin_station, destination_station)

                        if train_key in train_lookup:
                            train_id = train_lookup[train_key]
                        else:
                            cursor.execute("""
                                        INSERT INTO dim_train (filter_flags, trip_type, owner, trip_category, train_number,
                                        origin_station, destination_station)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        RETURNING train_id
                                        """, train_key)
                            train_id = cursor.fetchone()[0]
                            train_lookup[train_key] = train_id
                        connection.commit()

                        # Load fact into fact table (fact incomplete)
                        cursor.execute("""
                                       INSERT INTO fact_train_movements (stop_id, station_id, train_id, date_id, time_id,
                                       planned_arrival_time, planned_departure_time, cancelled, planned_platform)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       ON CONFLICT (stop_id) DO NOTHING
                                       """, (stop_id, station_id, train_id, date_id, time_id,
                                             planned_ar_time, planned_dp_time, cancelled, planned_platform))
                        connection.commit()



def extract_timetable_changes_from_tar(connection, cursor, tar_dir):

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
                            
                                cursor.execute("""
                                            UPDATE fact_train_movements
                                            SET changed_arrival_time = %s
                                            WHERE stop_id = %s
                                            RETURNING planned_arrival_time;
                                            """, (changed_ar_time, stop_id))
                                
                                row = cursor.fetchone()
                                # Case 1: row is none. That means, no row with this stop_id exists in fact_train_movements (unmatched change)
                                if row is None:
                                    unmatched = True
                                # Case 2: row[0] is not None and changed_ar_time exists. In that case, compute arrival_delay_minutes
                                elif row[0] is not None:
                                    planned_ar_time = row[0]
                                    diff = get_minute_diff(planned_ar_time, changed_ar_time)

                                    cursor.execute("""
                                                UPDATE fact_train_movements
                                                SET arrival_delay_minutes = %s
                                                WHERE stop_id = %s;
                                                """, (diff, stop_id))
                                # Case 3: row[0] is None. The stop is known, but the original timetable did not contain an arrival time
                                # Do nothing.

                            
                            # Changed platform
                            changed_platform = ar.get("cp")

                            if changed_platform is not None:
                                cursor.execute("""
                                           UPDATE fact_train_movements
                                           SET changed_platform = %s
                                           WHERE stop_id = %s;
                                           """, (changed_platform, stop_id))

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
                                
                                cursor.execute("""
                                            UPDATE fact_train_movements
                                            SET changed_departure_time = %s
                                            WHERE stop_id = %s
                                            RETURNING planned_departure_time;
                                            """, (changed_dp_time, stop_id))
                                
                                # Compute departure_delay_minutes
                                row = cursor.fetchone()
                                # Case 1: row is none. That means, no row with this stop_id exists in fact_train_movements (unmatched change)
                                if row is None:
                                    unmatched = True
                                # Case 2: row[0] is not None and changed_dp_time exists. In that case, compute arrival_delay_minutes
                                elif row[0] is not None:
                                    planned_dp_time = row[0]
                                    diff = get_minute_diff(planned_dp_time, changed_dp_time)

                                    cursor.execute("""
                                                UPDATE fact_train_movements
                                                SET departure_delay_minutes = %s
                                                WHERE stop_id = %s;
                                                """, (diff, stop_id))
                                # Case 3: row[0] is None. The stop is known, but the original timetable did not contain a departure time
                                # Do nothing.
                            
                                # Changed platform
                                changed_platform = dp.get("cp")

                                if changed_platform is not None:
                                    cursor.execute("""
                                            UPDATE fact_train_movements
                                            SET changed_platform = %s
                                            WHERE stop_id = %s;
                                            """, (changed_platform, stop_id))

                        # Cancellation
                        if cancelled == True:
                            cursor.execute("""
                                            UPDATE fact_train_movements
                                            SET cancelled = TRUE
                                            WHERE stop_id = %s;
                                            """, (stop_id, ))
                        # Timetable changes whose stop_id could not be matched to a planned movement are stored separately for traceability
                        if unmatched:
                            cursor.execute("""
                                            INSERT INTO unmatched_timetable_changes (
                                            stop_id, station_name, eva_number, changed_arrival_time, changed_departure_time, cancelled, changed_platform)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                                            """, (stop_id, root.get("station"), root.get("eva"), changed_ar_time, changed_dp_time, cancelled, changed_platform))
                           
                        connection.commit()
    

def main():
    # Connect to database
    connection = psycopg.connect(
        dbname="DBahn-berlin",
        user="postgres",
        password="postgres_password",
        host="localhost"
    )
    cursor = connection.cursor()

    weeks = ["250902_250909", "250909_250916", "250916_250923", "250923_250930", "250930_251007", "251007_251014", "251014_251021"]

    # ETL
    print("Extracting station data...")
    extract_station_data(connection=connection, cursor=cursor)

    # Create lookup tables for faster checking
    cursor.execute("SELECT name, station_id FROM dim_station")
    station_lookup = {row[0]: row[1] for row in cursor.fetchall()}

    for w in weeks:    
        print("Extracting timetables data...")
        tar_dir_planned = "datasets/DBahn-berlin/timetables/" + w + ".tar.gz"
        extract_timetables_from_tar(connection=connection, cursor=cursor, tar_dir=tar_dir_planned)

        print("Extracting timetable changes data...")
        tar_dir_changes = "datasets/DBahn-berlin/timetable_changes/" + w + ".tar.gz"
        extract_timetable_changes_from_tar(connection=connection, cursor=cursor, tar_dir=tar_dir_changes)

    cursor.close()
    connection.close()

    print("ETL done!")              


if __name__ == "__main__":
    main()
