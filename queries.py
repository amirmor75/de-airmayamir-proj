from trino.dbapi import connect

# Connection setup
conn = connect(
    host="trino.de.bsmch.net",
    port=80,
    user="hiveuser",
    catalog="hive",
    schema="airmayamir_bronze",
    http_scheme="http"
)

ACCOUNT = "dataengineering2025sa2"
data_container = "final-project-data"
silver_prefix = "airmayamir/silver"
bronze_prefix = "airmayamir/bronze"  # Correct bronze path

STAGING_LOCATION = f'abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/{bronze_prefix}/checkin/'
FINAL_LOCATION = f'abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/{silver_prefix}/checkin_parquet/'

source_table = 'checkin_csv_staging'
final_table = 'checkin_parquet'

cursor = conn.cursor()

print("Dropping existing tables if any...")
cursor.execute(f"DROP TABLE IF EXISTS {source_table}")
cursor.execute(f"DROP TABLE IF EXISTS {final_table}")

print("Creating staging table...")
cursor.execute(f"""
CREATE TABLE {source_table} (
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
    external_location = '{STAGING_LOCATION}',
    format = 'CSV',
    skip_header_line_count = 1,
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")

print("Refreshing partitions in staging table...")
cursor.execute(f"MSCK REPAIR TABLE {source_table}")

print("Verifying rows in staging table...")
cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
staging_count = cursor.fetchone()[0]
print(f"Total rows found in staging table: {staging_count}")

if staging_count == 0:
    print("⚠️ Warning: No data found in staging table. Check your external location and partitions.")
else:
    print("Creating final parquet table from staging data...")
    cursor.execute(f"""
        CREATE TABLE {final_table}
        WITH (
            external_location = '{FINAL_LOCATION}',
            format = 'PARQUET'
        ) AS
        SELECT
            CAST(Date AS TIMESTAMP) AS Date,
            FlightNumber,
            CAST(DepartureDate AS TIMESTAMP) AS DepartureDate,
            CAST(PassengerID AS INTEGER) AS PassengerID,
            CAST(CheckinTime AS TIMESTAMP) AS CheckinTime,
            source,
            DATE_FORMAT(DATE_PARSE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d'), '%Y-%m-%d') AS load_date
        FROM {source_table}
    """)

    print("Querying sample data from final parquet table...")
    cursor.execute(f"SELECT * FROM {final_table} LIMIT 10")
    rows = cursor.fetchall()

    print("\n📦 Sample Data from Final Table:")
    for row in rows:
        print(row)
