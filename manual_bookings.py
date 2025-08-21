from trino.dbapi import connect

conn = connect(
    host="trino.de.bsmch.net",
    port=80,
    user="hiveuser",
    catalog="hive",
    schema="airmayamir_bronze",
    http_scheme="http"
)
cursor = conn.cursor()

ACCOUNT = "dataengineering2025sa2"
data_container = "final-project-data"
bronze_prefix = "airmayamir/bronze"

partition_year = '2025'
partition_month = '08'
partition_day = '18'
partition_source = 'blob'

flight_bookings_location = (
    f"abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/"
    f"{bronze_prefix}/flight_bookings/"
)

print("\nDropping flight_bookings_csv_staging if exists...")
cursor.execute("DROP TABLE IF EXISTS flight_bookings_csv_staging")

print("\nCreating flight_bookings_csv_staging table...")
cursor.execute(f"""
CREATE TABLE flight_bookings_csv_staging (
    Date VARCHAR,
    BookingID VARCHAR,
    FlightNumber VARCHAR,
    DepartureDate VARCHAR,
    Destination VARCHAR,
    Passengers VARCHAR,
    Price VARCHAR,
    BookingDate VARCHAR,
    FuelConsumption VARCHAR,
    UserID VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    source VARCHAR
)
WITH (
    external_location = '{flight_bookings_location}',
    format = 'CSV',
    skip_header_line_count = 1,
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")
print("Flight bookings staging table created.")

print("\nSyncing partitions metadata for flight_bookings_csv_staging...")
cursor.execute(f"CALL system.sync_partition_metadata('airmayamir_bronze', 'flight_bookings_csv_staging', 'ADD')")
print("Partitions synced.")

print("\nDropping flight_bookings_with_load_date if exists...")
cursor.execute("DROP TABLE IF EXISTS flight_bookings_with_load_date")

print("\nCreating flight_bookings_with_load_date table with typed columns...")
cursor.execute(f"""
CREATE TABLE flight_bookings_with_load_date AS
SELECT
    CAST(CAST(REPLACE(Date, 'T', ' ') AS TIMESTAMP) AS DATE) AS Date,
    BookingID,
    FlightNumber,
    CAST(CAST(REPLACE(DepartureDate, 'T', ' ') AS TIMESTAMP) AS DATE) AS DepartureDate,
    Destination,
    CAST(Passengers AS INTEGER) AS Passengers,
    CAST(Price AS DOUBLE) AS Price,
    CAST(CAST(REPLACE(BookingDate, 'T', ' ') AS TIMESTAMP) AS DATE) AS BookingDate,
    CAST(FuelConsumption AS DOUBLE) AS FuelConsumption,
    CAST(UserID AS INTEGER) AS UserID,
    year,
    month,
    day,
    source,
    DATE_PARSE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d') AS load_date
FROM flight_bookings_csv_staging

""")
print("Final flight bookings table created.")

print("\nFetching sample data from flight_bookings_with_load_date...")
cursor.execute("SELECT * FROM flight_bookings_with_load_date LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)
