/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_created') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        CAST(account_id_hashed AS STRING) AS account_id_hashed,
        CAST(user_id_hashed AS STRING) AS user_id_hashed,

        -- Details
        CAST(account_type AS STRING) AS account_type,
        

        -- Metadata
        CAST(created_ts AS TIMESTAMP) AS created_ts,
        
    FROM
        source_data

)

SELECT * FROM formatted