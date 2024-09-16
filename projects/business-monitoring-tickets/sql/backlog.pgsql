-- SELECT
--     t.day::date + interval '1 hour' * 0,
--     t.day::date + interval '1 hour' * 7
-- FROM   generate_series(timestamp '2004-03-07'
--                      , timestamp '2004-08-16'
--                      , interval  '1 day') AS t(day);

SELECT Updater_id
FROM tickets
WHERE CREATED_AT < timestamp '2024-04-20'; 