{{
  config(
    materialized='incremental',
    unique_key='closed_ts',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
    
  )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_closed') }}
    {% if is_incremental() %}
        WHERE closed_ts >= (
            SELECT dateadd(day, -3, max(closed_ts)) 
            FROM {{ this }}
        )
    {% endif %}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        CAST(account_id_hashed AS STRING) AS account_id_hashed,

        -- Metadata
        CAST(closed_ts AS TIMESTAMP) AS closed_ts,
        
    FROM
        source_data

)

SELECT * FROM formatted
{{ add_rows_limit() }} 