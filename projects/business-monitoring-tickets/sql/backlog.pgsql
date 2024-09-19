-- Backlog Calculation
-- This query calculates the number of tickets in the backlog (tickets in the "New"
-- or "Open" status) at 07:00 AM and 00:00 AM each day. Data extraction spans from
-- 2024-04-15 00:00:00 to 2024-04-30 23:59:59. Include tickets that started before the
-- starting date of the extraction period (Only when it is possible).
WITH
    time_backlog AS (
        -- Generate a time series for 00:00:00 and 07:00:00 each day from 2024-04-15 to 2024-04-30
        SELECT generate_series(
            '2024-04-15 00:00:00'::timestamp,  -- start time
            '2024-04-30 23:59:59'::timestamp,  -- end time
            '1 day'::interval                  -- step of 1 day
        ) + time_entry AS "Time_Series"
        FROM (VALUES ('00:00:00'::time), ('07:00:00'::time)) AS time_entries(time_entry)
        ORDER BY "Time_Series"
    ),
    tickets_history AS(
        SELECT
            tb."Time_Series",
            t."TICKET_ID",
            t."VALUE_STATUS",
            -- Compute the row number of each record when it's partitioned by TICKET_ID and ordered by CREATED_AT in descending order
            -- This will number the status record in creation order for each TICKET_ID
            row_number() OVER (PARTITION BY t."TICKET_ID" ORDER BY t."CREATED_AT" DESC) as "Row_Number"
        -- Join each time_backlog record with all tickets records created before the time backlog entry
        FROM time_backlog tb
        LEFT JOIN tickets t
        ON t."CREATED_AT" <= tb."Time_Series"
    ),
    filtered_tickets AS (
        -- Filter by keeping only the first row for each TICKET_ID
        -- to get the latest status for each ticket at each timestamp
        SELECT
            "Time_Series",
            "TICKET_ID",
            "VALUE_STATUS"
        FROM tickets_history
        WHERE "Row_Number" = 1
)
SELECT
    -- Group by Time Series
    "Time_Series" as "Date",
    -- Count each record when the VALUE_STATUS matches 'open' or 'new'
    COUNT(CASE WHEN "VALUE_STATUS" = 'open' THEN 1 END) AS "Tickets_Open",
    COUNT(CASE WHEN "VALUE_STATUS" = 'new' THEN 1 END) AS "Tickets_New",
    COUNT(CASE WHEN "VALUE_STATUS" IN ('new', 'open') THEN 1 END) AS "Total"
FROM filtered_tickets
GROUP BY "Time_Series"