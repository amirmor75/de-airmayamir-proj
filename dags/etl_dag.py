from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fetch the Trino connection ID from Airflow Variables
trino_conn_id = Variable.get("TRINO_CONN_ID")  # <-- Defined in Airflow UI

with DAG(
    dag_id='airmayamir',
    default_args=default_args,
    description='airmayamir',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['trino', 'iceberg', 'hive', 'dedup', 'hash']
) as dag:

    dim_payment_methods = TrinoOperator(
        task_id='dim_payment_methods',
        sql="""
            INSERT INTO iceberg.analytics.dim_payment_method (
                payment_method_desc,
                payment_method_code
            )
            SELECT
                pm.transactionType AS payment_method_desc,
                xxhash64(pm.transactionType) AS payment_method_code
            FROM (
                SELECT DISTINCT transactionType
                FROM hive.payme.payments
                WHERE load_date >= DATE '{{ ds }}'
                AND load_date < DATE '{{ next_ds }}'
            ) pm
            WHERE NOT EXISTS (
                SELECT 1
                FROM iceberg.analytics.dim_payment_method existing
                WHERE existing.payment_method_desc = pm.transactionType
            );
        """,
        trino_conn_id=trino_conn_id
    )


    dim_currency = TrinoOperator(
        task_id='dim_currency',
        sql="""
            INSERT INTO iceberg.analytics.dim_currency (
                currency_name,
                currency_code,
                exchange_rate
            )
            SELECT
                new_currencies.currency AS currency_name,
                xxhash64(new_currencies.currency) AS currency_code,
                rates.rate AS exchange_rate
            FROM (
                SELECT DISTINCT currency, paymentDate
                FROM hive.payme.payments
                WHERE load_date >= DATE '{{ ds }}'
                AND load_date < DATE '{{ next_ds }}'
            ) new_currencies
            LEFT JOIN (
                SELECT currency, rate, rate_date
                FROM hive.boi.exchange_rates
            ) rates
            ON new_currencies.currency = rates.currency
            AND new_currencies.paymentDate = rates.rate_date
            WHERE NOT EXISTS (
                SELECT 1
                FROM iceberg.analytics.dim_currency existing
                WHERE existing.currency_name = new_currencies.currency
            );
        """,
        trino_conn_id=trino_conn_id
    )


    dim_user = TrinoOperator(
    task_id='dim_user',
    sql="""
        INSERT INTO iceberg.analytics.dim_user (
            userId
        )
        SELECT DISTINCT fb.userId
        FROM hive.flight.bookings fb
        WHERE fb.userId IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM iceberg.analytics.dim_user existing
            WHERE existing.userId = fb.userId
        );
    """,
    trino_conn_id=trino_conn_id
    )


    dim_time = TrinoOperator(
    task_id='dim_time',
    sql="""
        WITH date_range AS (
            SELECT
                DATE_ADD('month', -1, DATE(min(booking_date))) AS start_date,
                DATE '{{ ds }}' AS end_date
            FROM hive.flight.bookings
        ),
        calendar AS (
            SELECT
                DATE_ADD('day', x, start_date) AS full_date
            FROM date_range,
                 UNNEST(SEQUENCE(0, DATE_DIFF('day', start_date, end_date))) AS t(x)
        )
        INSERT INTO iceberg.analytics.dim_time (
            date_key,
            full_date,
            year,
            month_num,
            month_name,
            week_of_year,
            week_label,
            week_start_date,
            weekday_num,
            weekday_name,
            is_weekend
        )
        SELECT
            CAST(DATE_FORMAT(full_date, '%Y%m%d') AS INTEGER) AS date_key,
            full_date,
            YEAR(full_date),
            MONTH(full_date),
            DATE_FORMAT(full_date, '%M') AS month_name,
            WEEK_OF_YEAR(full_date),
            FORMAT('%s-W%s', YEAR(full_date), LPAD(CAST(WEEK_OF_YEAR(full_date) AS VARCHAR), 2, '0')) AS week_label,
            DATE_TRUNC('week', full_date) AS week_start_date,
            DAY_OF_WEEK(full_date),
            DATE_FORMAT(full_date, '%W') AS weekday_name,
            CASE WHEN DAY_OF_WEEK(full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM calendar
        WHERE NOT EXISTS (
            SELECT 1
            FROM iceberg.analytics.dim_time existing
            WHERE existing.date_key = CAST(DATE_FORMAT(full_date, '%Y%m%d') AS INTEGER)
        );
    """,
    trino_conn_id=trino_conn_id
    )
    

    dim_airline = TrinoOperator(
        task_id='dim_airline',
        sql="""
            INSERT INTO iceberg.analytics.dim_airline (
                airline_code
            )
            SELECT DISTINCT
                airline_code
            FROM (
                SELECT
                    regexp_extract(flight_number, '^[A-Z]+') AS airline_code
                FROM hive.flight.flights
                WHERE flight_number IS NOT NULL
                AND load_date >= DATE '{{ ds }}'
                AND load_date < DATE '{{ next_ds }}'
            ) extracted
            WHERE airline_code IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM iceberg.analytics.dim_airline existing
                WHERE existing.airline_code = extracted.airline_code
            );
        """,
        trino_conn_id=trino_conn_id
    )


    stg_payments = TrinoOperator(
    task_id='stg_payments',
    sql="""
        INSERT INTO iceberg.staging.stg_payments (
            payment_id,
            booking_id,
            user_id,
            payment_method_code,
            currency_code,
            base_currency,
            paid_date_key,
            paid_at_timestamp,
            amount,
            price_in_base_currency
        )
        SELECT
            xxhash64(p.user_id || DATE_FORMAT(DATE(p.paymentDate), '%Y-%m-%d')) AS payment_id,
            b.booking_id,
            p.user_id,
            pm.payment_method_code,
            dc.currency_code,
            'ILS' AS base_currency,
            CAST(DATE_FORMAT(p.paymentDate, '%Y%m%d') AS INT) AS paid_date_key,
            p.paymentDate AS paid_at_timestamp,
            p.amount,
            p.amount * COALESCE(dc.exchange_rate, 1) AS price_in_base_currency
        FROM hive.payme.payments p
        LEFT JOIN hive.flight.bookings b
            ON p.user_id = b.user_id
           AND DATE(p.paymentDate) = DATE(b.booking_date)
        LEFT JOIN iceberg.analytics.dim_payment_method pm
            ON pm.payment_method_desc = p.transactionType
        LEFT JOIN iceberg.analytics.dim_currency dc
            ON dc.currency_name = p.currency
        WHERE p.load_date >= DATE '{{ ds }}'
          AND p.load_date < DATE '{{ next_ds }}';
    """,
    trino_conn_id=trino_conn_id
    )



    stg_flights = TrinoOperator(
    task_id='stg_flights',
    sql="""
        WITH bookings_filtered AS (
            SELECT
                flight_number,
                DATE(departureDate) AS flight_date,
                departureDate as scheduled_departure,
                destination
            FROM hive.flight.bookings
            WHERE load_date >= DATE '{{ ds }}'
              AND load_date < DATE '{{ next_ds }}'
        ),

        flights_filtered AS (
            SELECT
                flight_number,
                departuredate,
                seats_booked,
                fuel_consumption,
                fuel_price as fuel_price_per_unit,
                crewMembers
            FROM hive.flight.flights
            WHERE load_date >= DATE '{{ ds }}'
              AND load_date < DATE '{{ next_ds }}'
        ),

        joined_flights AS (
            SELECT
                b.flight_number,
                b.flight_date,
                b.scheduled_departure,
                b.destination,
                f.seats_booked,
                f.fuel_consumption,
                f.fuel_price_per_unit,
                f.crewMembers,
                MAX(f.departureDate) AS actual_departure
            FROM bookings_filtered b
            LEFT JOIN flights_filtered f
              ON b.flight_number = f.flight_number
             AND DATE(f.departureDate) = b.flight_date
            GROUP BY
                b.flight_number,
                b.flight_date,
                b.scheduled_departure,
                b.destination,
                f.seats_booked,
                f.fuel_consumption,
                f.fuel_price_per_unit,
                f.crewMembers
        ),

        with_delay AS (
            SELECT
                flight_number,
                flight_date,
                scheduled_departure,
                actual_departure,
                destination,
                seats_booked,
                fuel_consumption,
                fuel_price_per_unit,
                crew_members_count,
                CAST(MINUTES_BETWEEN(scheduled_departure, actual_departure) AS INT) AS delay_minutes,
                xxhash64(flight_number || DATE_FORMAT(flight_date, '%Y-%m-%d')) AS flight_sk,
                uuid() AS flight_id,
                CAST(DATE_FORMAT(flight_date, '%Y%m%d') AS INT) AS date_key
            FROM joined_flights
        ),

        with_destination_code AS (
            SELECT
                w.*,
                dd.destination_code
            FROM with_delay w
            LEFT JOIN iceberg.analytics.dim_destination dd
              ON w.destination = dd.destination_name
        ),

        with_popularity_flag AS (
            SELECT
                w.*,
                CASE WHEN freq.count_by_number > freq.avg_count THEN TRUE ELSE FALSE END AS popular_ind
            FROM with_destination_code w
            LEFT JOIN (
                SELECT
                    flight_number,
                    COUNT(*) AS count_by_number,
                    AVG(COUNT(*)) OVER () AS avg_count
                FROM hive.flight.bookings
                WHERE load_date >= DATE '{{ ds }}'
                  AND load_date < DATE '{{ next_ds }}'
                GROUP BY flight_number
            ) freq ON w.flight_number = freq.flight_number
        )

        INSERT INTO iceberg.staging.stg_flights (
            flight_sk,
            flight_id,
            flight_number,
            date_key,
            destination_code,
            scheduled_departure_ts,
            actual_departure_ts,
            delay_minutes,
            seats_booked,
            fuel_consumption_units,
            fuel_price_per_unit,
            crew_members_count,
            popular_ind
        )
        SELECT
            flight_sk,
            flight_id,
            flight_number,
            date_key,
            destination_code,
            scheduled_departure,
            actual_departure,
            delay_minutes,
            seats_booked,
            fuel_consumption,
            fuel_price_per_unit,
            crew_members_count,
            popular_ind
        FROM with_popularity_flag;
    """,
    trino_conn_id=trino_conn_id
    )
