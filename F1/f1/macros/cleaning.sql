{% macro handle_null(value) %}
    -- Standard null cleaning for text-like inputs
    NULLIF(NULLIF({{ value }}::text,'\N'),'null')
{% endmacro %}


{% macro safe_cast(value, dtype) %}
    -- Clean, then safely cast to required type
    CASE
        WHEN {{ handle_null(value) }} IS NULL THEN NULL
        ELSE CAST({{ handle_null(value) }} AS {{ dtype }})
    END
{% endmacro %}


{% macro default_zero(value) %}
    -- Default numeric fields to zero if they are null or empty
    COALESCE({{ safe_cast(value, 'int') }}, 0)
{% endmacro %}


{% macro default_value(value, default) %}
    -- Default any value to a specific default when null
    COALESCE({{ handle_null(value) }}, '{{ default }}')
{% endmacro %}


{% macro clean_boolean(value) %}
    -- Normalize common boolean formats
    CASE
        WHEN LOWER({{ handle_null(value) }}) IN ('t', 'true', '1', 'yes') THEN TRUE
        WHEN LOWER({{ handle_null(value) }}) IN ('f', 'false', '0', 'no') THEN FALSE
        ELSE NULL
    END
{% endmacro %}


{% macro handle_null_time(value) %}
    -- Clean out null-like time entries
    NULLIF(
        NULLIF(
            NULLIF(
                TRIM({{ value }}),
            ''),
        '\N'),
    'null')
{% endmacro %}


{% macro handle_time_cast(value) %}
    -- Safely cast cleaned value into TIME datatype
    CASE
        WHEN {{ handle_null_time(value) }} IS NULL THEN NULL
        ELSE CAST({{ handle_null_time(value) }} AS time)
    END
{% endmacro %}


{% macro handle_time_safely(value) %}
    -- Validate time formatting AND cast safely
    CASE
        WHEN {{ handle_null_time(value) }} IS NULL THEN NULL
        WHEN {{ handle_null_time(value) }} ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$'
            THEN CAST({{ handle_null_time(value) }} AS time)
        ELSE NULL
    END
{% endmacro %}

{% macro convert_mixed_time(col) %}
(
    CASE
        WHEN {{ col }} IS NULL OR {{ col }} IN ('', '\N') THEN NULL

        -- Case: mm:ss.ms (e.g., 1:00.02)
        WHEN {{ col }} LIKE '%:%' THEN
            (
                split_part({{ col }}, ':', 1)::float * 60
                +
                split_part(
                    split_part({{ col }}, ':', 2),
                    '.',
                    1
                )::float
                +
                COALESCE(
                    ('0.' || split_part(split_part({{ col }}, ':', 2), '.', 2))::float,
                    0
                )
            )

        -- Case: ss.ms (e.g., 26.898, 23.1)
        WHEN {{ col }} LIKE '%.%' THEN
            {{ col }}::float

        -- Case: whole seconds (rare but possible)
        ELSE
            {{ col }}::float
    END
)
{% endmacro %}
