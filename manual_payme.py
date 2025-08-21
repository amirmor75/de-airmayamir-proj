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

payme_location = (
    f"abfss://{data_container}@{ACCOUNT}.dfs.core.windows.net/"
    f"{bronze_prefix}/payme/"
)

print("\nDropping payme_json_staging if exists...")
cursor.execute("DROP TABLE IF EXISTS payme_json_staging")

print("\nCreating payme_json_staging table...")
cursor.execute(f"""
CREATE TABLE payme_json_staging (
    PaymentDate VARCHAR,
    UserID VARCHAR,
    TransactionType VARCHAR,
    Amount DOUBLE,
    Currency VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR,
    source VARCHAR
)
WITH (
    external_location = '{payme_location}',
    format = 'JSON',
    partitioned_by = ARRAY['year', 'month', 'day', 'source']
)
""")
print("payme_json_staging table created.")

print("\nSyncing partitions metadata for payme_json_staging...")
cursor.execute(f"CALL system.sync_partition_metadata('airmayamir_bronze', 'payme_json_staging', 'ADD')")
print("Partitions synced.")

print("\nDropping payme_with_load_date if exists...")
cursor.execute("DROP TABLE IF EXISTS payme_with_load_date")

print("\nCreating payme_with_load_date table with typed columns...")
cursor.execute(f"""
CREATE TABLE payme_with_load_date AS
SELECT
    CAST(PaymentDate AS TIMESTAMP) AS PaymentDate,
    CAST(UserID AS INTEGER) AS UserID,
    TransactionType,
    Amount,
    Currency,
    year,
    month,
    day,
    source,
    DATE_PARSE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d') AS load_date
FROM payme_json_staging
WHERE PaymentDate IS NOT NULL
  AND PaymentDate NOT LIKE '{{{{%'  -- filter out invalid JSON blobs if any
  AND year = '{partition_year}'
  AND month = '{partition_month}'
  AND day = '{partition_day}'
  AND source = '{partition_source}'
""")
print("payme_with_load_date table created.")

print("\nFetching sample data from payme_with_load_date...")
cursor.execute("SELECT * FROM payme_with_load_date LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)
