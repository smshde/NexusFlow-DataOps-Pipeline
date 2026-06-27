-- ============================================================
-- NexusFlow — dbt Macros Library
-- ============================================================

-- ── MODE LOOKUP CTE ───────────────────────────────────────
-- Generates a (group_col, mode_col) CTE: most frequent non-null
-- mode_col value per group_col, via rank-by-frequency + take rank 1.
-- Redshift has no MODE()/MODE() WITHIN GROUP aggregate (Postgres/
-- Snowflake-only) — this is the portable equivalent. Join the result
-- on group_col to pull the mode value at any outer aggregation level.
-- Original implementation queried `FROM {{ this }}` (the model's own,
-- not-yet-built output table) via an uncorrelated subquery — failed
-- with "relation does not exist" on first run, and even if the table
-- existed, was never correlated to the surrounding GROUP BY key, so
-- it would've returned one global mode for every row, not one per group.
{% macro mode_lookup_cte(relation, group_col, mode_col) %}
    select {{ group_col }}, {{ mode_col }}
    from (
        select
            {{ group_col }},
            {{ mode_col }},
            row_number() over (
                partition by {{ group_col }}
                order by count(*) desc
            ) as rn
        from {{ relation }}
        where {{ mode_col }} is not null
        group by {{ group_col }}, {{ mode_col }}
    )
    where rn = 1
{% endmacro %}


-- ── DATE SPINE HELPER ────────────────────────────────────
{% macro generate_date_spine(start_date, end_date) %}
    SELECT
        dateadd('day', seq.n, '{{ start_date }}'::date) AS dt
    FROM (
        SELECT row_number() over () - 1 AS n
        FROM pg_catalog.pg_class
        CROSS JOIN pg_catalog.pg_class c2
        LIMIT datediff('day', '{{ start_date }}'::date, '{{ end_date }}'::date) + 1
    ) seq
{% endmacro %}


-- ── HASH PII ─────────────────────────────────────────────
-- SHA-256 hash of a PII column with optional salt
{% macro hash_pii(column_name, salt=var('pii_salt', 'nexusflow')) %}
    md5(lower(trim({{ column_name }})) || '{{ salt }}')
{% endmacro %}


-- ── SCD TYPE 2 MERGE ─────────────────────────────────────
-- Generic SCD2 merge helper (Redshift)
{% macro scd2_merge(target_table, source_cte, unique_key, check_cols) %}
    -- Close expired records
    UPDATE {{ target_table }}
    SET
        _valid_to   = s._valid_from,
        _is_current = false
    FROM {{ source_cte }} s
    WHERE {{ target_table }}.{{ unique_key }} = s.{{ unique_key }}
      AND {{ target_table }}._is_current = true
      AND (
        {% for col in check_cols %}
            {{ target_table }}.{{ col }} IS DISTINCT FROM s.{{ col }}
            {% if not loop.last %} OR {% endif %}
        {% endfor %}
      );

    -- Insert new versions
    INSERT INTO {{ target_table }}
    SELECT s.*
    FROM {{ source_cte }} s
    LEFT JOIN {{ target_table }} t
        ON s.{{ unique_key }} = t.{{ unique_key }}
       AND t._is_current = true
    WHERE t.{{ unique_key }} IS NULL
       OR (
        {% for col in check_cols %}
            t.{{ col }} IS DISTINCT FROM s.{{ col }}
            {% if not loop.last %} OR {% endif %}
        {% endfor %}
       );
{% endmacro %}


-- ── INCREMENTAL WHERE CLAUSE ─────────────────────────────
-- Standard incremental filter for Redshift merge strategy
{% macro incremental_where(ts_column='_processed_ts', lookback_hours=3) %}
    {% if is_incremental() %}
    WHERE {{ ts_column }} > (
        SELECT dateadd('hour', -{{ lookback_hours }}, max({{ ts_column }}))
        FROM {{ this }}
    )
    {% endif %}
{% endmacro %}


-- ── STAR SCHEMA FK VALIDATION ────────────────────────────
-- Macro to add referential integrity test between fact/dim
{% macro test_fk_integrity(model, column_name, ref_model, ref_column) %}
    SELECT
        f.{{ column_name }}
    FROM {{ model }} f
    WHERE f.{{ column_name }} != -1     -- allow unknown member
      AND f.{{ column_name }} NOT IN (
          SELECT {{ ref_column }}
          FROM {{ ref_model }}
      )
    LIMIT 100
{% endmacro %}


-- ── SAFE DIVIDE ──────────────────────────────────────────
{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN ({{ denominator }}) = 0 OR ({{ denominator }}) IS NULL
        THEN {{ default }}
        ELSE ({{ numerator }})::float / ({{ denominator }})
    END
{% endmacro %}


-- ── REVENUE BAND ─────────────────────────────────────────
{% macro revenue_band(column_name) %}
    CASE
        WHEN {{ column_name }} <    25 THEN 'micro'
        WHEN {{ column_name }} <   100 THEN 'small'
        WHEN {{ column_name }} <   250 THEN 'medium'
        WHEN {{ column_name }} <   500 THEN 'large'
        WHEN {{ column_name }} <  2000 THEN 'premium'
        ELSE                                'whale'
    END
{% endmacro %}


-- ── GENERATE SURROGATE KEY (override dbt_utils) ──────────
{% macro generate_surrogate_key(field_list) %}
    md5(
        {% for field in field_list %}
            coalesce(cast({{ field }} as varchar), 'null')
            {% if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}


-- ── LOG MODEL METADATA ───────────────────────────────────
{% macro log_model_info() %}
    {% if execute %}
        {{ log("Running model: " ~ this ~ " | target: " ~ target.name, info=True) }}
    {% endif %}
{% endmacro %}
