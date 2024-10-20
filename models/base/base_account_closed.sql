/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_closed') }}

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