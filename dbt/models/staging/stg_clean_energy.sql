SELECT
    CAST(utc_timestamp AS TIMESTAMP) AS utc_timestamp,
    CAST(cet_cest_timestamp AS TIMESTAMP) AS cet_cest_timestamp,

    -- National load & forecast
    CAST(de_load_actual_entsoe_transparency AS FLOAT64) AS de_load_actual,
    CAST(de_load_forecast_entsoe_transparency AS FLOAT64) AS de_load_forecast,

    -- Solar
    CAST(de_solar_capacity AS FLOAT64) AS de_solar_capacity,
    CAST(de_solar_generation_actual AS FLOAT64) AS de_solar_generation,
    CAST(de_solar_profile AS FLOAT64) AS de_solar_profile,

    -- Wind (overall)
    CAST(de_wind_capacity AS FLOAT64) AS de_wind_capacity,
    CAST(de_wind_generation_actual AS FLOAT64) AS de_wind_generation,
    CAST(de_wind_profile AS FLOAT64) AS de_wind_profile,

    -- Wind (offshore)
    CAST(de_wind_offshore_capacity AS FLOAT64) AS de_wind_offshore_capacity,
    CAST(de_wind_offshore_generation_actual AS FLOAT64) AS de_wind_offshore_generation,
    CAST(de_wind_offshore_profile AS FLOAT64) AS de_wind_offshore_profile,

    -- Wind (onshore)
    CAST(de_wind_onshore_capacity AS FLOAT64) AS de_wind_onshore_capacity,
    CAST(de_wind_onshore_generation_actual AS FLOAT64) AS de_wind_onshore_generation,
    CAST(de_wind_onshore_profile AS FLOAT64) AS de_wind_onshore_profile,

    -- 50Hertz region
    CAST(de_50hertz_load_actual_entsoe_transparency AS FLOAT64) AS de_50hertz_load_actual,
    CAST(de_50hertz_load_forecast_entsoe_transparency AS FLOAT64) AS de_50hertz_load_forecast,
    CAST(de_50hertz_solar_generation_actual AS FLOAT64) AS de_50hertz_solar_generation,
    CAST(de_50hertz_wind_generation_actual AS FLOAT64) AS de_50hertz_wind_generation,
    CAST(de_50hertz_wind_offshore_generation_actual AS FLOAT64) AS de_50hertz_wind_offshore_generation,
    CAST(de_50hertz_wind_onshore_generation_actual AS FLOAT64) AS de_50hertz_wind_onshore_generation,

    -- Amprion region
    CAST(de_amprion_load_actual_entsoe_transparency AS FLOAT64) AS de_amprion_load_actual,
    CAST(de_amprion_load_forecast_entsoe_transparency AS FLOAT64) AS de_amprion_load_forecast,
    CAST(de_amprion_solar_generation_actual AS FLOAT64) AS de_amprion_solar_generation,
    CAST(de_amprion_wind_onshore_generation_actual AS FLOAT64) AS de_amprion_wind_onshore_generation,

    -- TenneT region
    CAST(de_tennet_load_actual_entsoe_transparency AS FLOAT64) AS de_tennet_load_actual,
    CAST(de_tennet_load_forecast_entsoe_transparency AS FLOAT64) AS de_tennet_load_forecast,
    CAST(de_tennet_solar_generation_actual AS FLOAT64) AS de_tennet_solar_generation,
    CAST(de_tennet_wind_generation_actual AS FLOAT64) AS de_tennet_wind_generation,
    CAST(de_tennet_wind_offshore_generation_actual AS FLOAT64) AS de_tennet_wind_offshore_generation,
    CAST(de_tennet_wind_onshore_generation_actual AS FLOAT64) AS de_tennet_wind_onshore_generation,

    -- TransnetBW region
    CAST(de_transnetbw_load_actual_entsoe_transparency AS FLOAT64) AS de_transnetbw_load_actual,
    CAST(de_transnetbw_load_forecast_entsoe_transparency AS FLOAT64) AS de_transnetbw_load_forecast,
    CAST(de_transnetbw_solar_generation_actual AS FLOAT64) AS de_transnetbw_solar_generation,
    CAST(de_transnetbw_wind_onshore_generation_actual AS FLOAT64) AS de_transnetbw_wind_onshore_generation

FROM
    `energy-forecast-pipeline.cleaned_data.clean_data`
