/*
    Tables
*/

WITH account_created AS (

    SELECT * FROM {{ ref('base_account_closed') }}

),


/*
    Transformations
*/

deduplicated_closed_event as (

    SELECT
       account_id_hashed,

       closed_ts,
        
    FROM
        account_created
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY account_id_hashed, CAST(closed_ts AS DATE)  -- Group by account and closure day
            ORDER BY closed_ts DESC  -- Rank by closure timestamp (latest first)
        ) =1
)

/*
    Formatted
*/

formatted AS (
    SELECT
      account_id_hashed,
      closed_ts,
    FROM eduplicated_closed_event
)

SELECT * FROM formatted