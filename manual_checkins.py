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

checkin_location = (
    f"abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/"
    f"{bronze_prefix}/checkin/"
)

print("\nDropping checkin_csv_staging if exists...")
cursor.execute("DROP TABLE IF EXISTS checkin_csv_staging")

print("\nCreating checkin_csv_staging table...")
cursor.execute(f"""
CREATE TABLE checkin_csv_staging (
    Date VARCHAR,
    FlightNumber VARCHAR,
    DepartureDate VARCHAR,
    PassengerID VARCHAR,
    CheckinTime VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    source VARCHAR
)
WITH (
    external_location = '{checkin_location}',
    format = 'CSV',
    skip_header_line_count = 1,
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")
print("Table created.")

# Instead of ALTER TABLE ADD PARTITION, just sync partitions metadata
print("\nSyncing partitions metadata for checkin_csv_staging...")
cursor.execute(f"CALL system.sync_partition_metadata('airmayamir_bronze', 'checkin_csv_staging', 'ADD')")
print("Partitions synced.")

# Then you can create your final table with casted date columns etc.
print("\nDropping checkin_with_load_date if exists...")
cursor.execute("DROP TABLE IF EXISTS checkin_with_load_date")

print("\nCreating checkin_with_load_date table with parsed dates...")
cursor.execute(f"""
CREATE TABLE checkin_with_load_date AS
SELECT
    CAST(CAST(Date AS TIMESTAMP) AS DATE) AS Date,
    FlightNumber,
    CAST(CAST(DepartureDate AS TIMESTAMP) AS DATE) AS DepartureDate,
    PassengerID,
    CAST(CAST(CheckinTime AS TIMESTAMP) AS DATE) AS CheckinTime,
    year,
    month,
    day,
    source,
    DATE_PARSE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d') AS load_date
FROM checkin_csv_staging
""")

print("Final table created.")

print("\nFetching sample data...")
cursor.execute("SELECT * FROM checkin_with_load_date LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)
