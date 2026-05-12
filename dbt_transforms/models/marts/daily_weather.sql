with staging as (

    select * from {{ ref('stg_weather') }}

),

daily_aggregates as (

    select
        city_name,
        country_code,
        cast(ingested_at as date)               as weather_date,
        round(avg(temperature_c), 2)            as avg_temp_c,
        round(avg(temperature_f), 2)            as avg_temp_f,
        round(min(temperature_c), 2)            as min_temp_c,
        round(max(temperature_c), 2)            as max_temp_c,
        round(avg(feels_like_c),  2)            as avg_feels_like_c,
        round(avg(humidity_pct), 1)             as avg_humidity_pct,
        min(humidity_pct)                       as min_humidity_pct,
        max(humidity_pct)                       as max_humidity_pct,
        round(avg(wind_speed_ms), 2)            as avg_wind_speed_ms,
        round(max(wind_speed_ms), 2)            as max_wind_speed_ms,
        mode() within group (
            order by weather_category
        )                                       as dominant_weather,
        count(*)                                as record_count

    from staging
    group by
        city_name,
        country_code,
        cast(ingested_at as date)

)

select * from daily_aggregates
