WITH ranked_closures AS (
   SELECT
       account_id_hashed,
       closed_ts,
       ROW_NUMBER() OVER (
           PARTITION BY account_id_hashed, CAST(closed_ts AS DATE)
           ORDER BY closed_ts DESC
       ) AS row_num
   FROM `analytics-take-home-test.monzo_datawarehouse.account_closed`
)

, valid_closures AS (
   SELECT
       rc.account_id_hashed,
       rc.closed_ts
   FROM ranked_closures rc
   WHERE rc.row_num = 1
)

, closure_reopen_pairs AS (
   SELECT
       ac.account_id_hashed,
       ac.user_id_hashed,
       ac.created_ts,
       ac.account_type,
       vc.closed_ts,
       MIN(ro.reopened_ts) AS reopened_ts
   FROM `analytics-take-home-test.monzo_datawarehouse.account_created` ac
   LEFT JOIN valid_closures vc ON ac.account_id_hashed = vc.account_id_hashed
   LEFT JOIN `analytics-take-home-test.monzo_datawarehouse.account_reopened` ro
       ON ac.account_id_hashed = ro.account_id_hashed
       AND ro.reopened_ts > vc.closed_ts
   GROUP BY ac.account_id_hashed, ac.user_id_hashed, ac.created_ts, vc.closed_ts, ac.account_type
)

, account_lifecycle AS (
   SELECT
       cr.account_id_hashed,
       cr.user_id_hashed,
       cr.created_ts,
       cr.closed_ts,
       cr.reopened_ts,
       cr.account_type,
       CASE
           WHEN cr.closed_ts IS NULL THEN 'open'
           WHEN cr.reopened_ts IS NOT NULL THEN 'open'
           ELSE 'closed'
       END AS account_status
   FROM closure_reopen_pairs cr
)

, open_accounts AS (
   SELECT
       al.user_id_hashed,
       al.account_id_hashed,
       al.created_ts,
       al.closed_ts,
       al.account_status
   FROM account_lifecycle al
   WHERE al.account_status = 'open'
)

-- Now we generate a list of unique dates (calculation dates)
, unique_transaction_dates AS (
   SELECT DISTINCT CAST(tr.date AS DATE) AS calculation_date
   FROM `analytics-take-home-test.monzo_datawarehouse.account_transactions` tr
)

, transactions_last_7_days AS (
   SELECT
       uad.calculation_date,
       oa.user_id_hashed,
       COUNT(DISTINCT oa.account_id_hashed) AS open_accounts,
       SUM(CASE 
               WHEN DATE(tr.date) BETWEEN DATE_SUB(uad.calculation_date, INTERVAL 7 DAY) AND uad.calculation_date
               THEN 1 ELSE 0
           END) AS transactions_in_last_7_days
   FROM unique_transaction_dates uad
   JOIN open_accounts oa
       ON uad.calculation_date BETWEEN CAST(oa.created_ts AS DATE) AND COALESCE(CAST(oa.closed_ts AS DATE), uad.calculation_date)
   LEFT JOIN `analytics-take-home-test.monzo_datawarehouse.account_transactions` tr
       ON oa.account_id_hashed = tr.account_id_hashed
   GROUP BY uad.calculation_date, oa.user_id_hashed
)

, active_user_rate AS (
   SELECT
       t.calculation_date,
       COUNT(DISTINCT CASE WHEN t.transactions_in_last_7_days > 0 THEN t.user_id_hashed END) AS active_users,
       COUNT(DISTINCT t.user_id_hashed) AS total_open_users,
       COUNT(DISTINCT CASE WHEN t.transactions_in_last_7_days > 0 THEN t.user_id_hashed END) / 
       COUNT(DISTINCT t.user_id_hashed) AS seven_day_active_user_rate
   FROM transactions_last_7_days t
   GROUP BY t.calculation_date
)

SELECT *
FROM  transactions_last_7_days
WHERE user_id_hashed = 'CPhWQbkFSCK4nHlWNZYXnEKr5DpowKwUIDPeclqifhI8pE8enCy8CsAD3AYbgI/n/b6RNzPFraSB760fahkStg=='
--and calculation_date= '2020-07-05'

