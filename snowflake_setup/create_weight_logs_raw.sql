create or replace table weight_logs_raw (
    id string, -- Notino page ID
    date date, -- Date of the weight log
    weight_kg float, -- Weight in kilograms
    time_of_day string -- Time of day when the weight was recorded (e.g., "morning", "evening")
)