--# of users who installed the app
SELECT COUNT(DISTINCT user_pseudo_id) as users_installed
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'

--Daily breakdown of number of users who installed the app
SELECT 
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date,
COUNT(DISTINCT user_pseudo_id) as users_installed
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
GROUP BY date
ORDER BY date

--Top 10 country where the installers come from
WITH
--Compute for the numerators
country_counts AS (
SELECT
geo.country,
COUNT(DISTINCT user_pseudo_id) AS users
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
AND geo.country <> ""
GROUP BY geo.country
),
--Compute for the denominators
user_counts AS (
SELECT
COUNT(DISTINCT user_pseudo_id)
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
),
--Compute for the percentages
percent AS (
SELECT
country,
ROUND(users / (SELECT * FROM user_counts), 4) AS percent_users
FROM country_counts
)

SELECT * FROM percent
ORDER BY percent_users DESC
LIMIT 10

--Device categories of the installers
WITH
device_counts AS (
SELECT
device.category,
COUNT(DISTINCT user_pseudo_id) AS users
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
AND device.category <> ""
GROUP BY device.category
),

user_counts AS (
SELECT
COUNT(DISTINCT user_pseudo_id)
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
),

percent AS (
SELECT
category,
ROUND(users / (SELECT * FROM user_counts), 4) AS percent_users
FROM device_counts
)

SELECT * FROM percent
ORDER BY percent_users DESC

--Daily active users
WITH
daily_user_count AS (
SELECT
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date,
COUNT(DISTINCT user_pseudo_id) AS active_users
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "user_engagement"
AND _TABLE_SUFFIX BETWEEN '20180901' and '20180930'
GROUP BY date
)

SELECT AVG(active_users) AS daily_active_users
FROM daily_user_count

--Revenue earned over the time period
SELECT SUM(user_ltv.revenue) AS revenue
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "in_app_purchase"
AND geo.country = "United States"
AND _TABLE_SUFFIX BETWEEN '20180901' and '20180930'

--# of users who experienced crashes
SELECT COUNT(DISTINCT user_pseudo_id) AS users
FROM `firebase-public-project.analytics_153293282.events_*`,
UNNEST(event_params) e
WHERE event_name = 'app_exception'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20180930'
AND e.key = 'fatal' AND e.value.int_value = 1

--% of users that are still with the app after 7 days
WITH
--List of users who installed in Sept
sept_cohort AS (
SELECT DISTINCT user_pseudo_id,
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date_first_open,
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'first_open'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20180930'
),
--Get the list of users who uninstalled
uninstallers AS (
SELECT DISTINCT user_pseudo_id,
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date_app_remove,
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'app_remove'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20181007'
),
--Join the 2 tables and compute for # of days to uninstall
joined AS (
SELECT a.*,
b.date_app_remove,
DATE_DIFF(DATE(b.date_app_remove), DATE(a.date_first_open), DAY) AS days_to_uninstall
FROM sept_cohort a
LEFT JOIN uninstallers b
ON a.user_pseudo_id = b.user_pseudo_id
)

SELECT
COUNT(DISTINCT
CASE WHEN days_to_uninstall > 7 OR days_to_uninstall IS NULL THEN user_pseudo_id END) /
COUNT(DISTINCT user_pseudo_id)
AS percent_users_7_days
FROM joined

--% of users who experienced crashes among uninstallers
WITH
--List of users who installed in Sept
sept_cohort AS (
SELECT DISTINCT user_pseudo_id,
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date_first_open,
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'first_open'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20180930'
),
--Get the list of users who uninstalled
uninstallers AS (
SELECT DISTINCT user_pseudo_id,
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date_app_remove,
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = 'app_remove'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20181007'
),
--Get the list of users who experienced crashes
users_crashes AS (
SELECT DISTINCT user_pseudo_id,
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS date_crash,
FROM `firebase-public-project.analytics_153293282.events_*`,
UNNEST(event_params) e
WHERE event_name = 'app_exception'
AND _TABLE_SUFFIX BETWEEN '20180901' and '20181007'
AND e.key = 'fatal' AND e.value.int_value = 1
),
--Join the 3 tables
joined AS (
SELECT a.*,
b.date_app_remove,
DATE_DIFF(DATE(b.date_app_remove), DATE(a.date_first_open), DAY) AS days_to_uninstall,
c.date_crash
FROM sept_cohort a
LEFT JOIN uninstallers b
ON a.user_pseudo_id = b.user_pseudo_id
LEFT JOIN users_crashes c
ON a.user_pseudo_id = c.user_pseudo_id
)

SELECT
COUNT(DISTINCT
CASE WHEN days_to_uninstall <= 7 AND date_crash IS NOT NULL
THEN user_pseudo_id END)
/ COUNT(DISTINCT
CASE WHEN days_to_uninstall <= 7 THEN user_pseudo_id END)
AS percent_users_crashes
FROM joined