create or replace table WEIGHT_DB.RAW.weight_logs_raw_temp (
    id string, -- Notion page ID
    date date, -- Date of the weight log
    weight_kg float, -- Weight in kilograms
    time_of_day string, -- Time of day when the weight was recorded (e.g., "morning", "evening")
    loaded_at timestamp_ltz default current_timestamp() -- Timestamp when the record was loaded to DB
)