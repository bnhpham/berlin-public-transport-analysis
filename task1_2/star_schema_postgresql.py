import psycopg


def create_tables():
    # Connect to the PostgreSQL database
    connection = psycopg.connect(
        dbname="DBahn-berlin",
        user="postgres",
        password="postgres_password",
        host="localhost"
    )
    cursor = connection.cursor()

    # If we want to setup a new database, delete old tables
    cursor.execute("DROP TABLE IF EXISTS fact_train_movements")
    connection.commit()
    cursor.execute("DROP TABLE IF EXISTS dim_station")
    connection.commit()
    cursor.execute("DROP TABLE IF EXISTS dim_train")
    connection.commit()
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    connection.commit()
    cursor.execute("DROP TABLE IF EXISTS dim_time")
    connection.commit()
    cursor.execute("DROP TABLE IF EXISTS unmatched_timetable_changes")
    connection.commit()

    # Create station table
    cursor.execute("""
        CREATE TABLE dim_station (
            station_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            number VARCHAR(100),
            ifopt VARCHAR(100),
            name VARCHAR(100),
            city VARCHAR(100),
            zipcode VARCHAR(100),
            street VARCHAR(100),
            category INT,
            priceCategory INT,
            hasParking BOOLEAN,
            hasBicycleParking BOOLEAN,
            hasLocalPublicTransport BOOLEAN,
            hasPublicFacilities BOOLEAN,
            hasLockerSystem BOOLEAN,
            hasTaxiRank BOOLEAN,
            hasTravelNecessities BOOLEAN,
            hasSteplessAccess VARCHAR(100),
            hasMobilityService BOOLEAN,
            hasWiFi BOOLEAN,
            hasTravelCenter BOOLEAN,
            hasRailwayMission BOOLEAN,
            hasDBLounge BOOLEAN,
            hasLostAndFound BOOLEAN,
            hasCarRental BOOLEAN,
            federalState VARCHAR(100),
            regionalbereich_number INT,
            regionalbereich_name VARCHAR(100),
            regionalbereich_shortName VARCHAR(100),
            aufgabentraeger_shortName VARCHAR(100),
            aufgabentraeger_name VARCHAR(100),
            timeTableOffice_email VARCHAR(100),
            timeTableOffice_name VARCHAR(100),
            szentrale_number INT,
            szentrale_publicPhoneNumber VARCHAR(100),
            szentrale_name VARCHAR(100),
            stationManagement_number INT,
            stationManagement_name VARCHAR(100),
            eva_number INT UNIQUE,
            longitude DECIMAL(9, 6),
            latitude DECIMAL(9, 6),
            rilIdentifier VARCHAR(100),
            rilIdentifier_hasSteamPermission BOOLEAN,
            rilIdentifier_steamPermission VARCHAR(100),
            rilIdentifier_longitude DECIMAL(9, 6),
            rilIdentifier_latitude DECIMAL(9, 6),
            rilIdentifier_primaryLocationCode VARCHAR(100),
            productLine VARCHAR(100),
            segment VARCHAR(100)
        )
    """)
    connection.commit()
    print("Station table created successfully.")

    # Create train table
    cursor.execute("""
        CREATE TABLE dim_train (
            train_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            filter_flags VARCHAR(100),
            trip_type VARCHAR(5),
            owner VARCHAR(100),
            trip_category VARCHAR(100),
            train_number INT,
            origin_station VARCHAR(100),
            destination_station VARCHAR(100),
                   
            CONSTRAINT unique_train UNIQUE (filter_flags, trip_type, trip_category, owner, train_number, origin_station, destination_station)
        )
    """)
    connection.commit()
    print("Train table created successfully.")

    # Create date table
    cursor.execute("""
        CREATE TABLE dim_date (
            date_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            date DATE,
                   
            CONSTRAINT unique_date UNIQUE (date)
        )
    """)
    connection.commit()
    print("Date table created successfully.")

    # Create the time table
    cursor.execute("""
        CREATE TABLE dim_time (
            time_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            time TIME,
                   
            CONSTRAINT unique_time UNIQUE (time)
        )
    """)
    connection.commit()
    print("Time table created successfully.")

    # Create table for unmatched timetable changes
    cursor.execute("""
        CREATE TABLE unmatched_timetable_changes (
            change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            stop_id VARCHAR(100),
            station_name VARCHAR(100),
            eva_number INT,
            changed_arrival_time TIMESTAMP,
            changed_departure_time TIMESTAMP,
            cancelled BOOLEAN,
            changed_platform VARCHAR(100)
        )
    """)
    connection.commit()
    print("Table for unmatched timetable changes created successfully.")

    # Create fact table (train movements)
    cursor.execute("""
                   CREATE TABLE fact_train_movements (
                   movement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                   stop_id VARCHAR(100) UNIQUE,
                   station_id BIGINT REFERENCES dim_station(station_id),
                   train_id BIGINT REFERENCES dim_train(train_id),
                   date_id BIGINT REFERENCES dim_date(date_id),
                   time_id BIGINT REFERENCES dim_time(time_id),
                   planned_arrival_time TIMESTAMP WITHOUT TIME ZONE,
                   planned_departure_time TIMESTAMP WITHOUT TIME ZONE,
                   changed_arrival_time TIMESTAMP WITHOUT TIME ZONE,
                   changed_departure_time TIMESTAMP WITHOUT TIME ZONE,
                   arrival_delay_minutes INT,
                   departure_delay_minutes INT,
                   cancelled BOOLEAN,
                   planned_platform VARCHAR(100),
                   changed_platform VARCHAR(100)
                   )
                   """)
    connection.commit()
    print("Fact table created successfully.")
    
    # Test if date table was actually created
    # INSERT
    cursor.execute("""
    INSERT INTO dim_date (date)
    VALUES ('2025-09-02')
    """)
    connection.commit()
    # SELECT
    cursor.execute("""
    SELECT * FROM dim_date
    """)
    print(cursor.fetchall()) # Should return "[(1, datetime.date(2025, 9, 2))]"
    # DELETE
    cursor.execute("""
    DELETE FROM dim_date
    WHERE date='2025-09-02'
    """)
    connection.commit()

    cursor.close()
    connection.close()


if __name__ == "__main__":
    create_tables()