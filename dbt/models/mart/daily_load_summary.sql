-- models/mart/daily_load_summary.sql

SELECT
    DATE(utc_timestamp) AS date,

    -- Load values
    AVG(de_load_actual) AS avg_actual_load,
    AVG(de_load_forecast) AS avg_forecast_load,

    -- Solar generation
    SUM(de_solar_generation) AS total_solar_generation,
    AVG(de_solar_profile) AS avg_solar_profile,

    -- Wind generation
    SUM(de_wind_generation) AS total_wind_generation,
    AVG(de_wind_profile) AS avg_wind_profile,
    SUM(de_wind_onshore_generation) AS total_wind_onshore,
    SUM(de_wind_offshore_generation) AS total_wind_offshore,

    -- Regional data: 50Hertz
    SUM(de_50hertz_load_actual) AS total_50hertz_load,
    SUM(de_50hertz_solar_generation) AS total_50hertz_solar,
    SUM(de_50hertz_wind_generation) AS total_50hertz_wind

{{ ref('stg_clean_energy') }}

GROUP BY date
ORDER BY date
