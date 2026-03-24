USE ROLE SYSADMIN;
USE DATABASE SANDBOX;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA CONSUMPTION_SCH;

CREATE OR REPLACE TABLE CONSUMPTION_SCH.DATE_DIM (
    DATE_DIM_HK NUMBER PRIMARY KEY,
    CALENDAR_DATE DATE UNIQUE,
    YEAR NUMBER,
    QUARTER NUMBER,
    MONTH NUMBER,
    WEEK NUMBER,
    DAY_OF_YEAR NUMBER,
    DAY_OF_WEEK NUMBER,
    DAY_OF_THE_MONTH NUMBER,
    DAY_NAME STRING
);

MERGE INTO CONSUMPTION_SCH.DATE_DIM AS target
USING (
    WITH RECURSIVE date_bounds AS (
        SELECT
            COALESCE(MIN(DATE(order_date)), CURRENT_DATE - 365) AS start_date,
            CURRENT_DATE + 365 AS end_date
        FROM CLEAN_SCH.ORDERS
    ),
    date_series AS (
        SELECT start_date AS calendar_date
        FROM date_bounds

        UNION ALL

        SELECT DATEADD(day, 1, calendar_date)
        FROM date_series, date_bounds
        WHERE calendar_date < end_date
    )
    SELECT
        HASH(SHA1_HEX(calendar_date)) AS date_dim_hk,
        calendar_date,
        YEAR(calendar_date) AS year,
        QUARTER(calendar_date) AS quarter,
        MONTH(calendar_date) AS month,
        WEEKOFYEAR(calendar_date) AS week,
        DAYOFYEAR(calendar_date) AS day_of_year,
        DAYOFWEEK(calendar_date) AS day_of_week,
        DAY(calendar_date) AS day_of_the_month,
        DAYNAME(calendar_date) AS day_name
    FROM date_series
) source
ON target.CALENDAR_DATE = source.calendar_date
WHEN NOT MATCHED THEN
INSERT (
    DATE_DIM_HK,
    CALENDAR_DATE,
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY_OF_YEAR,
    DAY_OF_WEEK,
    DAY_OF_THE_MONTH,
    DAY_NAME
)
VALUES (
    source.date_dim_hk,
    source.calendar_date,
    source.year,
    source.quarter,
    source.month,
    source.week,
    source.day_of_year,
    source.day_of_week,
    source.day_of_the_month,
    source.day_name
);

