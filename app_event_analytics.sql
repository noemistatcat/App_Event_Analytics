--How many installed the app? (single number)
SELECT DISTINCT user_id
FROM `firebase-public-project.analytics_153293282.events_*`
WHERE event_name = "first_open"
AND _TABLE_SUFFIX BETWEEN '20180927' and '20181003'
