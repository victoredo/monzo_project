/*
    Tables
*/

WITH account_created AS (

    SELECT * FROM {{ ref('base_account_created') }}

),

account_closed AS (

    SELECT * FROM {{ ref('int_account_closed') }}

),

account_reopened AS (

    SELECT * FROM {{ ref('base_account_reopened') }}

),


account_transaction AS (

    SELECT * FROM {{ ref('base_account_transactions') }}

),
/*
    Transformations
*/

-- 1. Prepare Account Lifecycle Data
, closure_reopen_pairs AS (
    SELECT
        account_created.account_id_hashed,
        account_created.user_id_hashed,
        account_created.created_ts,
        account_created.account_type,
        account_closed.closed_ts,
        MIN(account_reopened.reopened_ts) AS reopened_ts  -- Find the first reopen event after each close
    FROM account_created AS  ac
    LEFT JOIN account_closed as vc 
        ON account_created.account_id_hashed = account_closed.account_id_hashed
    LEFT JOIN account_reopened  ro 
        ON account_created.account_id_hashed = account_reopened.account_id_hashed
        AND account_reopened.reopened_ts > account_closed.closed_ts  -- Only consider reopen events after the closure
    GROUP BY ALL
)


-- 2. Flag Accounts with Multiple Closures Without Corresponding Reopen Events
, multiple_closures_without_reopen AS (
    SELECT
        cr.account_id_hashed,
        cr.user_id_hashed,
        COUNT(cr.closed_ts) AS num_closures,  -- Count how many closure events each account has
        SUM(CASE WHEN cr.reopened_ts IS NULL THEN 1 ELSE 0 END) AS num_unreopened_closures  -- Count closures without reopen
    FROM closure_reopen_pairs cr
    GROUP BY cr.account_id_hashed, cr.user_id_hashed
    HAVING COUNT(cr.closed_ts) > 1  -- Only flag if there are multiple closures
)

-- 3. Determine Account Status Based on Closure-Reopen Cycles
, account_lifecycle AS (
    SELECT
        cr.account_id_hashed,
        cr.user_id_hashed,
        cr.created_ts,
        cr.closed_ts,
        cr.reopened_ts,
        cr.account_type,
        CASE
            -- If no closure, account is open
            WHEN cr.closed_ts IS NULL THEN 'open'
            -- If account was reopened after being closed, it's open
            WHEN cr.reopened_ts IS NOT NULL THEN 'open'
            -- If account was closed but not reopened, it's closed
            ELSE 'closed'
        END AS account_status
    FROM closure_reopen_pairs cr
)

-- 4. Aggregate Transaction Data
, transaction_summary AS (
    SELECT
        account_id_hashed,
        SUM(transactions_num) AS total_transactions
    FROM `analytics-take-home-test.monzo_datawarehouse.account_transactions`
    GROUP BY account_id_hashed
),

-- 5. Create  Unified Account Model and Flag Multiple Closures Without Reopen Events
Unified_Account_Model as (
SELECT
    al.account_id_hashed,
    al.user_id_hashed,
    al.account_type,
    al.created_ts,
    al.closed_ts,
    al.reopened_ts,
    al.account_status,
    COALESCE(ts.total_transactions, 0) AS total_transactions,
    CASE
        WHEN mcr.num_unreopened_closures > 0 THEN 1 ELSE 0  -- Flag if there are multiple closures without reopening
    END AS multiple_closures_without_reopen_flag
FROM account_lifecycle al
LEFT JOIN transaction_summary ts ON al.account_id_hashed = ts.account_id_hashed
LEFT JOIN multiple_closures_without_reopen mcr ON al.account_id_hashed = mcr.account_id_hashed
)
select *
from Unified_Account_Model


