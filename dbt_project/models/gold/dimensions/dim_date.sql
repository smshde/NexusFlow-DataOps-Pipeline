{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension', 'date']
  )
}}

-- dim_date
-- Date dimension spine from 2023-01-01 to 2030-12-31

with spine as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2023-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}
),

final as (
    select
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_sk,
        date_day                                        as full_date,
        extract(year  from date_day)::int               as year,
        extract(month from date_day)::int               as month,
        extract(day   from date_day)::int               as day,
        extract(dow   from date_day)::int               as day_of_week,
        extract(doy   from date_day)::int               as day_of_year,
        extract(week  from date_day)::int               as week_of_year,
        extract(quarter from date_day)::int             as quarter,
        to_char(date_day, 'Month')                      as month_name,
        to_char(date_day, 'Day')                        as day_name,
        case when extract(dow from date_day) in (0,6)
             then true else false end                   as is_weekend,
        case when extract(month from date_day) in (11,12,1,2)
             then true else false end                   as is_holiday_season
    from spine
)

select * from final
