with source as (

    select * from raw_weather

),

cleaned as (

    select
        id                                        as weather_id,
        city                                      as city_name,
        country                                   as country_code,
        cast(temperature  as numeric(5,2))        as temperature_c,
        cast(feels_like   as numeric(5,2))        as feels_like_c,
        cast(humidity     as integer)             as humidity_pct,
        cast(pressure     as integer)             as pressure_hpa,
        cast(wind_speed   as numeric(5,2))        as wind_speed_ms,
        weather_main                              as weather_category,
        weather_desc                              as weather_description,
        cast(ingested_at  as timestamp)           as ingested_at,
        source                                    as data_source,
        round((cast(temperature as numeric) * 9/5) + 32, 2) as temperature_f

    from source
    where city is not null
      and temperature is not null

)

select * from cleaned
