{{
  config(
    materialized='incremental',
    unique_key='reopened_ts',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
    
  )
}}
/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_reopened') }}
     {% if is_incremental() %}
        WHERE reopened_ts >= (
            SELECT dateadd(day, -3, max(reopened_ts)) 
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
        CAST(reopened_ts AS TIMESTAMP) AS reopened_ts,
        
    FROM
        source_data

)

SELECT * FROM formatted
{{ add_rows_limit() }} 