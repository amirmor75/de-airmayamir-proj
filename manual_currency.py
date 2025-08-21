from trino.dbapi import connect

# Connect to Trino
conn = connect(
    host="trino.de.bsmch.net",
    port=80,
    user="hiveuser",
    catalog="hive",
    schema="airmayamir_bronze",
    http_scheme="http"
)
cursor = conn.cursor()

# Storage info
ACCOUNT = "dataengineering2025sa2"
data_container = "final-project-data"
bronze_prefix = "airmayamir/bronze"

exchange_rates_location = (
    f"abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/"
    f"{bronze_prefix}/currency/"
)

partition_year = '2025'
partition_month = '08'
partition_day = '20'
partition_source = 'blob'

# 1. Drop staging table if exists
print("\nDropping exchange_rates_json_staging if exists...")
cursor.execute("DROP TABLE IF EXISTS exchange_rates_json_staging")

# 2. Create staging table over JSON files with array of exchangeRates
print("\nCreating exchange_rates_json_staging table...")
cursor.execute(f"""
CREATE TABLE exchange_rates_json_staging (
    exchangeRates ARRAY(
        ROW(
            key VARCHAR,
            currentExchangeRate DOUBLE,
            currentChange DOUBLE,
            unit INTEGER,
            lastUpdate VARCHAR
        )
    ),
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    source VARCHAR
)
WITH (
    external_location = '{exchange_rates_location}',
    format = 'JSON',
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")
print("exchange_rates_json_staging table created.")

# 3. Sync partitions metadata
print("\nSyncing partitions metadata for exchange_rates_json_staging...")
cursor.execute(f"CALL system.sync_partition_metadata('airmayamir_bronze', 'exchange_rates_json_staging', 'ADD')")
print("Partitions synced.")

# 4. Drop final table if exists
print("\nDropping exchange_rates_final if exists...")
cursor.execute("DROP TABLE IF EXISTS exchange_rates_final")

# 5. Create final table selecting the full array without unnesting
print("\nCreating exchange_rates_final table without unnesting...")
cursor.execute(f"""
CREATE TABLE exchange_rates_final AS
SELECT
    exchangeRates,
    year,
    month,
    day,
    source
FROM exchange_rates_json_staging
WHERE year = '{partition_year}'
  AND month = '{partition_month}'
  AND day = '{partition_day}'
  AND source = '{partition_source}'
""")
print("exchange_rates_final table created.")

# 6. Fetch sample data
print("\nFetching sample data from exchange_rates_final...")
cursor.execute("SELECT * FROM exchange_rates_final LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)
