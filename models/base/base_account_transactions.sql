{{
  config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}
/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_transactions') }}
    {% if is_incremental() %}
        WHERE date >= (
            SELECT dateadd(day, -3, max(date)) 
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
            
        -- Measures
        
        CAST(transactions_num AS INTEGER) AS transactions_num ,
       
        -- Metadata
        CAST(date AS TIMESTAMP) AS date,
        
    FROM
        source_data

)

SELECT * FROM formatted
{{ add_rows_limit() }} 