{{ config(
    materialized='incremental',
    unique_key = ['account_id_hashed', 'closed_ts','created_ts']
) }}

final as (
    SELECT

        -- FK
        account_id_hashed,
        user_id_hashed,

        -- Details
        account_type,
        account_status,

        -- Timestamps
        created_ts,
        closed_ts,
        reopened_ts,
        
        -- Metrics
        total_transactions,
        multiple_closures_without_reopen_flag
        
    FROM  {{ ref('int_accounts_enrichmentd') }}
    {% if is_incremental() %}
    -- Only include rows where the data is new or has changed
    WHERE created_ts >= (SELECT MAX(created_ts) FROM {{ this }} )
    OR closed_ts >= (SELECT MAX(closed_ts) FROM {{ this }} )
    OR reopened_ts >= (SELECT MAX(reopened_ts) FROM {{ this }} )
{% endif %}
        
)

SELECT * FROM final