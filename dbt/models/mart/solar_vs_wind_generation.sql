SELECT
    DATE(utc_timestamp) AS date,
    
    -- Solar Generation
    SUM(de_solar_generation) AS total_solar_generation,
    AVG(de_solar_profile) AS avg_solar_profile,
    
    -- Wind Generation
    SUM(de_wind_generation) AS total_wind_generation,
    AVG(de_wind_profile) AS avg_wind_profile,

    -- Onshore vs Offshore
    SUM(de_wind_onshore_generation) AS total_onshore_wind,
    SUM(de_wind_offshore_generation) AS total_offshore_wind

FROM
    {{ ref('stg_clean_energy') }}

GROUP BY
    date
ORDER BY
    date
