
{% docs mart_7_days_active_users %}

## mart_7_days_active_users

The model calculates the 7-day active user rate for any given date by first identifying users with open accounts and then counting how many of those users had at least one transaction within the previous 7 days.

`Account Lifecycle Data`: The model starts by combining account lifecycle events (creation, closure, and reopening) to determine the status of each account (open or closed).

`Open Accounts`: Only accounts that are open on the given calculation date are considered.

`Transaction Activity`: For each calculation date, the model checks whether users with open accounts had transactions within the last 7 days.

`Active User Rate`: The model computes the 7-day active user rate by dividing the number of active users by the total number of users with open accounts.
 
{% enddocs %}

