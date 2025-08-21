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

flight_location = (
    f"abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/"
    f"{bronze_prefix}/flight/"
)

print("\nDropping flight_booking_csv_staging if exists...")
cursor.execute("DROP TABLE IF EXISTS flight_booking_csv_staging")

print("\nCreating flight_booking_csv_staging table...")
cursor.execute(f"""
CREATE TABLE flight_booking_csv_staging (
    Date VARCHAR,
    FlightNumber VARCHAR,
    DepartureDate VARCHAR,
    SeatsBooked VARCHAR,
    FuelConsumption VARCHAR,
    FuelPrice VARCHAR,
    CrewMembers VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    source VARCHAR
)
WITH (
    external_location = '{flight_location}',
    format = 'CSV',
    skip_header_line_count = 1,
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")
print("Flight booking staging table created.")

print("\nSyncing partitions metadata for flight_booking_csv_staging...")
cursor.execute(f"CALL system.sync_partition_metadata('airmayamir_bronze', 'flight_booking_csv_staging', 'ADD')")
print("Partitions synced.")

print("\nDropping flight_booking_with_load_date if exists...")
cursor.execute("DROP TABLE IF EXISTS flight_booking_with_load_date")

print("\nCreating flight_booking_with_load_date table with typed columns...")
cursor.execute(f"""
CREATE TABLE flight_booking_with_load_date AS
SELECT
    CAST(CAST(Date AS TIMESTAMP) AS DATE) AS Date,
    FlightNumber,
    CAST(CAST(DepartureDate AS TIMESTAMP) AS DATE) AS DepartureDate,
    CAST(SeatsBooked AS INTEGER) AS SeatsBooked,
    CAST(FuelConsumption AS DOUBLE) AS FuelConsumption,
    CAST(FuelPrice AS DOUBLE) AS FuelPrice,
    CAST(CrewMembers AS INTEGER) AS CrewMembers,
    year,
    month,
    day,
    source,
    DATE_PARSE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d') AS load_date
FROM flight_booking_csv_staging
""")
print("Final flight booking table created.")

print("\nFetching sample data from flight_booking_with_load_date...")
cursor.execute("SELECT * FROM flight_booking_with_load_date LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)
