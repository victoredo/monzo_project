/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('analytics-take-home-test', 'account_transactions') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        CAST(account_id_hashed AS STRING) AS account_id_hashed,
        
        -- Details
        CAST(account_type AS STRING) AS account_type,
        
        -- Measures
        
        CAST(transactions_num AS INTEGER) AS transactions_num ,
       
        -- Metadata
        CAST(date AS TIMESTAMP) AS date,
        
    FROM
        source_data

)

SELECT * FROM formatted