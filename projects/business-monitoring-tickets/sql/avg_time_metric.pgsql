-- Average Time Metric
-- Calculate the average duration in minutes for the following metrics:
--  ● Total time of tickets: From status "New" to the first last status either "Solved"
--  or "Closed".
--  ● Waiting time: Total duration the ticket is in "New", "Open", or "Hold" (ticket_id,
--  Time in status new + time in status open + time in status hold).
--  ● First reply time: Time from "New" to another status, ensuring that the
-- Updater_id is different between the "New" status and the following not new
-- status used for the calculation. This check is to exclude cases where users
-- might create a message before receiving a reply from the company, changing
-- the ticket to open themself.
-- Before calculating the averages, exclude values that are less than 1 hour. For
-- instance, if a ticket was solved in 10 minutes, it should not be considered.For tickets
-- lacking either a creation date (status new) or a solved date (last status solved/closed)
-- within the extraction period, set the creation date as 2024-04-14 13:00:00 and the
-- solved date as 2024-05-01 13:00:00.

-- This first SQL query fills the tickets table with entries
-- for all missing 'new' or 'solved' status for each unique ticket
-- It uses default created_at timestamp for each status
WITH tickets_statuses AS (
    SELECT
        "TICKET_ID",
        -- List all VALUE_STATUS for each unique tickets
        array_agg("VALUE_STATUS") as statuses
    FROM tickets
    GROUP BY "TICKET_ID"
),
-- Table of tickets that don't have any 'new' in VALUE_STATUS
tickets_no_creation AS(
    SELECT *
    FROM tickets_statuses
    WHERE NOT ('new' = ANY ("statuses"))
),
-- Table of tickets that don't have any 'solved' or 'closed' in VALUE_STATUS
tickets_not_solved AS(
    SELECT *
    FROM tickets_statuses
    WHERE NOT ('solved' = ANY ("statuses") OR 'closed' = ANY ("statuses"))
)
INSERT INTO tickets
-- Add a new record with VALUE_STATUS = 'new' for each ticket without 'new' status
SELECT
    "TICKET_ID",
    'new',
    NULL,
    -1,
    '2024-04-14 13:00:00'::timestamp
FROM tickets_no_creation
UNION
-- Add a new record with VALUE_STATUS = 'solved' for each ticket without 'solved' and 'closed' status
SELECT
    "TICKET_ID",
    'solved',
    NULL,
    -1,
    '2024-05-01 13:00:00'::timestamp
FROM tickets_not_solved;

-- This is the main query to compute the average time metric
WITH
    -- Table with all unique tickets and their creation time
    tickets_creation AS(
        SELECT
            "TICKET_ID",
            "CREATED_AT" as creation_time
        from tickets
        -- Creation time is when status is 'new' 
        WHERE "VALUE_STATUS" = 'new'
    ),
    -- Table with all unique tickets and their closing time
    tickets_closing AS(
        SELECT
            "TICKET_ID",
            -- Closing time is the min timestamp when status is 'solved' or 'closed' 
            min("CREATED_AT") as closing_time
        from tickets
        WHERE 
            "VALUE_STATUS" = 'solved' OR
            "VALUE_STATUS" = 'closed'
        GROUP BY "TICKET_ID"
    ),
    -- Table which add the row number partitioned by ticket id and ordered by creation time
    -- This gives then the record/status creation order within each unique ticket id
    tickets_ordered AS(
        SELECT *,
        row_number() OVER (PARTITION BY "TICKET_ID" ORDER BY "CREATED_AT") as "Row_Number"
        FROM tickets
    ),
    -- Table with the added duration of the previous state.
    tickets_prev_state_duration AS(
        SELECT
            t_cur.*,
            extract(epoch from t_cur."CREATED_AT" - t_prev."CREATED_AT") / 60  as "Previous_State_Duration_minutes",
            t_prev."Updater_id" as "Previous_Updater_id"
        -- Joining twice the ordered ticket number on their ticket id and the row number - 1
        -- This allow to associate a record with its previous record and thus calculating the previous state duration
        FROM tickets_ordered t_cur
        LEFT JOIN tickets_ordered t_prev
        ON t_prev."TICKET_ID" = t_cur."TICKET_ID"
        AND t_prev."Row_Number" = t_cur."Row_Number" - 1
    ),
    -- Table of unique ticket with their waiting time
    tickets_waiting_time AS(
        SELECT "TICKET_ID",
            -- Sum the previous state duration when previous state is in ['new', 'open', 'hold']
            sum("Previous_State_Duration_minutes") FILTER (
                WHERE
                    "VALUE_PREVIOUS_VALUE" = ANY(ARRAY['new', 'open', 'hold']) 
            ) as "Waiting_Time"
        FROM tickets_prev_state_duration
        GROUP BY "TICKET_ID"
    ),
    -- Table of unique ticket with their first response time
    tickets_frp AS(
        SELECT "TICKET_ID",
            "Previous_State_Duration_minutes" as "FRT"
        FROM tickets_prev_state_duration
        -- Each ticket has only one previous value = 'new' since it can be in 'new' only once
        WHERE "VALUE_PREVIOUS_VALUE" = 'new'
        -- Make sure that the Updater id is not the same for the 'new' status and the following one
        AND "Previous_Updater_id" != "Updater_id"
    ),
    -- Table to store all the metric times for each tickets
    tickets_times AS(
        SELECT
            tcr."TICKET_ID",
            -- The tickets solved date is its closing time
            tcl."closing_time"::date as "Solved_Date",
            extract(epoch from tcl."closing_time" - tcr."creation_time") / 60 as "Total_Time",
            tw."Waiting_Time",
            tfrp."FRT"
        FROM tickets_creation tcr
        LEFT JOIN tickets_closing tcl ON tcr."TICKET_ID"=tcl."TICKET_ID"
        LEFT JOIN tickets_waiting_time tw ON tcr."TICKET_ID"=tw."TICKET_ID"
        LEFT JOIN tickets_frp tfrp ON tcr."TICKET_ID"=tfrp."TICKET_ID"
    )
-- Compute the average of each metric times when grouped by Solved_Date
SELECT "Solved_Date",
    round(avg("Total_Time") FILTER (
        WHERE
            "Total_Time" >= 60 
    ), 2) as "AVG_Total_Time",
    round(avg("Waiting_Time") FILTER (
        WHERE
            "Waiting_Time" >= 60 
    ), 2) as "AVG_Waiting_Time",
    round(avg("FRT") FILTER (
        WHERE
            "FRT" >= 60
    ), 2) as "AVG_FRT"
FROM tickets_times
WHERE  "Solved_Date" IS NOT NULL
GROUP BY "Solved_Date"
ORDER BY "Solved_Date"
