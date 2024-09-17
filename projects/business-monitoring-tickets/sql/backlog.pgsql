WITH
    time_backlog AS (
        -- Generate a time series for 00:00:00 and 07:00:00 each day from 2024-04-15 to 2024-04-30
        SELECT generate_series(
            '2024-04-15 00:00:00'::timestamp,  -- start time
            '2024-04-30 23:59:59'::timestamp,  -- end time
            '1 day'::interval                  -- step of 1 day
        ) + time_entry AS time_series
        FROM (VALUES ('00:00:00'::time), ('07:00:00'::time)) AS time_entries(time_entry)
        ORDER BY "time_series"
    ),
    tickets_history AS(
        SELECT
            tb."time_series",
            t."TICKET_ID",
            t."VALUE_STATUS",
            row_number() OVER (PARTITION BY t."TICKET_ID" ORDER BY t."CREATED_AT" DESC) as row_number
        FROM tickets t
        RIGHT JOIN time_backlog tb
        ON t."CREATED_AT" <= tb."time_series"
    ),
filtered_tickets AS (
    -- Filter to get the latest status for each ticket at each timestamp
    SELECT
        "time_series",
        "TICKET_ID",
        "VALUE_STATUS"
    FROM tickets_history
    WHERE row_number = 1
)
SELECT
    "time_series" as "Date",
    COUNT(CASE WHEN "VALUE_STATUS" = 'open' THEN 1 END) AS "Tickets_Open",
    COUNT(CASE WHEN "VALUE_STATUS" = 'new' THEN 1 END) AS "Tickets_New",
    COUNT(CASE WHEN "VALUE_STATUS" IN ('new', 'open') THEN 1 END) AS "Tickets_New"
FROM filtered_tickets
GROUP BY "time_series"