/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_reopened') }}

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