-- =====================================================
-- 🔹 SETUP
-- =====================================================
USE ROLE SYSADMIN;
USE DATABASE SANDBOX;

-- =====================================================
-- 🔹 STAGE TABLE
-- =====================================================
CREATE OR REPLACE TABLE STAGE_SCH.LOCATION (
    LOCATIONID TEXT,
    CITY TEXT,
    STATE TEXT,
    ZIPCODE TEXT,
    ACTIVEFLAG TEXT,
    CREATEDDATE TEXT,
    MODIFIEDDATE TEXT,
    _STG_FILE_NAME TEXT,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 TEXT,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 🔹 STREAMS (2 STREAMS)
-- =====================================================
CREATE OR REPLACE STREAM STAGE_SCH.LOCATION_STM_DQ 
ON TABLE STAGE_SCH.LOCATION
APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM STAGE_SCH.LOCATION_STM_TRANSFORM 
ON TABLE STAGE_SCH.LOCATION
APPEND_ONLY = TRUE;

-- =====================================================
-- 🔹 DQ ERROR TABLE
-- =====================================================
CREATE OR REPLACE TABLE STAGE_SCH.LOCATION_DQ_ERROR (
    LOCATIONID TEXT,
    CITY TEXT,
    STATE TEXT,
    ZIPCODE TEXT,
    ERROR_REASON TEXT,
    _STG_FILE_NAME TEXT,
    LOAD_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 🔹 SNOWPIPE
-- =====================================================
CREATE OR REPLACE PIPE STAGE_SCH.LOCATION_PIPE
AS
COPY INTO STAGE_SCH.LOCATION
FROM (
    SELECT 
        t.$1::TEXT,
        t.$2::TEXT,
        t.$3::TEXT,
        t.$4::TEXT,
        t.$5::TEXT,
        t.$6::TEXT,
        t.$7::TEXT,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP
    FROM @STAGE_SCH.CSV_STG/LOCATION t
)
FILE_FORMAT = (FORMAT_NAME = 'STAGE_SCH.CSV_FILE_FORMAT')
ON_ERROR = 'CONTINUE';

-- =====================================================
-- 🔹 CLEAN TABLE
-- =====================================================
CREATE OR REPLACE TABLE CLEAN_SCH.RESTAURANT_LOCATION (
    RESTAURANT_LOCATION_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    LOCATION_ID NUMBER NOT NULL UNIQUE,
    CITY STRING(100) NOT NULL,
    STATE STRING(100) NOT NULL,
    STATE_CODE STRING(2),
    IS_UNION_TERRITORY BOOLEAN,
    CAPITAL_CITY_FLAG BOOLEAN,
    CITY_TIER TEXT,
    ZIP_CODE STRING,
    ACTIVE_FLAG STRING,
    CREATED_TS TIMESTAMP_TZ,
    MODIFIED_TS TIMESTAMP_TZ,
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 🔹 CLEAN STREAM
-- =====================================================
CREATE OR REPLACE STREAM CLEAN_SCH.RESTAURANT_LOCATION_STM 
ON TABLE CLEAN_SCH.RESTAURANT_LOCATION;

-- =====================================================
-- 🔹 DQ TASK (STREAM 1)
-- =====================================================
CREATE OR REPLACE TASK STAGE_SCH.LOCATION_DQ_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('STAGE_SCH.LOCATION_STM_DQ')
AS
INSERT INTO STAGE_SCH.LOCATION_DQ_ERROR
SELECT 
    LOCATIONID,
    CITY,
    STATE,
    ZIPCODE,
    CASE 
        WHEN TRY_TO_NUMBER(LOCATIONID) IS NULL THEN 'INVALID LOCATIONID'
        WHEN CITY IS NULL THEN 'CITY NULL'
        WHEN STATE IS NULL THEN 'STATE NULL'
        WHEN ZIPCODE IS NULL THEN 'ZIPCODE NULL'
    END,
    _STG_FILE_NAME,
    CURRENT_TIMESTAMP
FROM STAGE_SCH.LOCATION_STM_DQ
WHERE 
    TRY_TO_NUMBER(LOCATIONID) IS NULL
    OR CITY IS NULL
    OR STATE IS NULL
    OR ZIPCODE IS NULL;

-- =====================================================
-- 🔹 TRANSFORM TASK (STREAM 2) 
-- =====================================================
CREATE OR REPLACE TASK STAGE_SCH.LOCATION_TRANSFORM_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('STAGE_SCH.LOCATION_STM_TRANSFORM')
AS
MERGE INTO CLEAN_SCH.RESTAURANT_LOCATION AS target
USING (
    SELECT 
        TRY_TO_NUMBER(LocationID) AS Location_ID,
        City,

        CASE 
            WHEN State = 'Delhi' THEN 'New Delhi'
            ELSE State
        END AS State,

        CASE 
           WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            ELSE NULL
        END AS state_code,

        CASE 
            WHEN State IN ('Delhi','Chandigarh','Puducherry','Jammu and Kashmir') THEN TRUE
            ELSE FALSE
        END AS is_union_territory,

        CASE 
            WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            ELSE FALSE
        END AS capital_city_flag,

        CASE 
            WHEN City IN ('Mumbai','Delhi','Bengaluru','Hyderabad','Chennai','Kolkata','Pune','Ahmedabad') THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                          'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,

        ZipCode,
        ActiveFlag,

        
        TRY_TO_TIMESTAMP_TZ(CreatedDate, 'DD-MM-YYYY HH24:MI') AS created_ts,
        TRY_TO_TIMESTAMP_TZ(ModifiedDate, 'DD-MM-YYYY HH24:MI') AS modified_ts,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts

    FROM STAGE_SCH.LOCATION_STM_TRANSFORM
    WHERE TRY_TO_NUMBER(LocationID) IS NOT NULL
) source

ON target.Location_ID = source.Location_ID

WHEN MATCHED THEN UPDATE SET
    target.City = source.City,
    target.State = source.State

WHEN NOT MATCHED THEN INSERT (
    Location_ID, City, State, state_code,
    is_union_territory, capital_city_flag, city_tier,
    Zip_Code, Active_Flag,
    created_ts, modified_ts,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
VALUES (
    source.Location_ID, source.City, source.State, source.state_code,
    source.is_union_territory, source.capital_city_flag, source.city_tier,
    source.ZipCode, source.ActiveFlag,
    source.created_ts, source.modified_ts,
    source._stg_file_name, source._stg_file_load_ts,
    source._stg_file_md5, source._copy_data_ts
);

-- =====================================================
-- 🔹 CONSUMPTION TABLE (UNCHANGED)
-- =====================================================
CREATE OR REPLACE TABLE CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM (
    RESTAURANT_LOCATION_HK NUMBER PRIMARY KEY,
    LOCATION_ID NUMBER,
    CITY VARCHAR(100),
    STATE VARCHAR(100),
    STATE_CODE VARCHAR(2),
    IS_UNION_TERRITORY BOOLEAN,
    CAPITAL_CITY_FLAG BOOLEAN,
    CITY_TIER VARCHAR(6),
    ZIP_CODE VARCHAR(10),
    ACTIVE_FLAG VARCHAR(10),
    EFF_START_DT TIMESTAMP,
    EFF_END_DT TIMESTAMP,
    CURRENT_FLAG BOOLEAN
);

-- =====================================================
-- 🔹 CONSUMPTION TASK 
-- =====================================================
CREATE OR REPLACE TASK CLEAN_SCH.LOCATION_DIM_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CLEAN_SCH.RESTAURANT_LOCATION_STM')
AS
MERGE INTO CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
USING CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
ON target.LOCATION_ID = source.LOCATION_ID

WHEN MATCHED AND source.METADATA$ACTION = 'DELETE'
THEN UPDATE SET 
    target.EFF_END_DT = CURRENT_TIMESTAMP(),
    target.CURRENT_FLAG = FALSE

WHEN NOT MATCHED THEN INSERT (
    RESTAURANT_LOCATION_HK,
    LOCATION_ID,
    CITY,
    STATE,
    STATE_CODE,
    IS_UNION_TERRITORY,
    CAPITAL_CITY_FLAG,
    CITY_TIER,
    ZIP_CODE,
    ACTIVE_FLAG,
    EFF_START_DT,
    EFF_END_DT,
    CURRENT_FLAG
)
VALUES (
    HASH(SHA1_HEX(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
    source.LOCATION_ID,
    source.CITY,
    source.STATE,
    source.STATE_CODE,
    source.IS_UNION_TERRITORY,
    source.CAPITAL_CITY_FLAG,
    source.CITY_TIER,
    source.ZIP_CODE,
    source.ACTIVE_FLAG,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
);

-- =====================================================
-- 🔹 RESUME TASKS
-- =====================================================
ALTER TASK STAGE_SCH.LOCATION_DQ_TASK RESUME;
ALTER TASK STAGE_SCH.LOCATION_TRANSFORM_TASK RESUME;
ALTER TASK CLEAN_SCH.LOCATION_DIM_TASK RESUME;

-- =====================================================
-- 🔹 RUN PIPELINE
-- =====================================================
ALTER PIPE STAGE_SCH.LOCATION_PIPE REFRESH;



