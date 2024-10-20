
/*
    Tables
*/

WITH accounts_enrichment AS (

    SELECT * FROM {{ ref('int_accounts_enrichment') }}
    WHERE account_status in  ('open', 'reopened')

),

account_transaction AS (

    SELECT * FROM {{ ref('base_account_transactions') }}

),

/*
    Transformations
*/

-- generate a list of unique dates (calculation dates)
unique_transaction_dates AS (
   SELECT 
     DISTINCT date AS calculation_date
   FROM account_transaction
),

transactions_last_7_days AS (
   SELECT
       unique_transaction_dates.calculation_date,
       accounts_enrichment.user_id_hashed,
       COUNT(DISTINCT accounts_enrichment.account_id_hashed) AS open_accounts,
       SUM(CASE 
               WHEN DATE(account_transaction.date) BETWEEN DATE_SUB(unique_transaction_dates.calculation_date, INTERVAL 7 DAY) AND unique_transaction_dates.calculation_date
               THEN 1 ELSE 0
           END) AS transactions_in_last_7_days
   FROM unique_transaction_dates 
   JOIN accounts_enrichment 
       ON unique_transaction_dates.calculation_date BETWEEN CAST(accounts_enrichment.created_ts AS DATE) AND COALESCE(CAST(accounts_enrichment.closed_ts AS DATE), unique_transaction_dates.calculation_date)
   LEFT JOIN account_transaction 
       ON accounts_enrichment.account_id_hashed = account_transaction.account_id_hashed
   GROUP BY ALL
),

active_user_rate AS (
   SELECT
       calculation_date,
       COUNT(DISTINCT CASE WHEN transactions_in_last_7_days > 0 THEN user_id_hashed END) AS active_users,
       COUNT(DISTINCT user_id_hashed) AS total_open_users,
       COUNT(DISTINCT CASE WHEN transactions_in_last_7_days > 0 THEN user_id_hashed END) / 
       COUNT(DISTINCT user_id_hashed) AS seven_day_active_user_rate
   FROM transactions_last_7_days 
   GROUP BY 1
)

SELECT 
    calculation_date,
    active_users,
    total_open_users,
    seven_day_active_user_rate
FROM  transactions_last_7_days
