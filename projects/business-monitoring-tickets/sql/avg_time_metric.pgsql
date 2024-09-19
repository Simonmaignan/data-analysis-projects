-- SELECT
--     "TICKET_ID",
--     "VALUE_STATUS",
--     "VALUE_PREVIOUS_VALUE",
--     "CREATED_AT"
-- FROM tickets
-- ORDER BY "TICKET_ID", "CREATED_AT"
WITH
    tickets_unique AS(
        SELECT DISTINCT "TICKET_ID"
        FROM tickets
    ),
    tickets_creation AS(
        SELECT
            "TICKET_ID",
            min("CREATED_AT") as creation_time
        from tickets
        WHERE "VALUE_STATUS" = 'new'
        GROUP BY "TICKET_ID"
    ),
    tickets_creation_full AS(
        SELECT
            tu."TICKET_ID",
            tc."creation_time"
            -- CASE WHEN "creation_time" IS NULL THEN
            --     '2024-04-14 13:00:00'::timestamp
            -- ELSE
            --     creation_time
            -- END AS creation_time
        FROM tickets_unique tu
        LEFT JOIN tickets_creation tc
        ON tu."TICKET_ID"=tc."TICKET_ID"
    ),
    tickets_closing AS(
        SELECT
            "TICKET_ID",
            min("CREATED_AT") as closing_time
        from tickets
        WHERE 
            "VALUE_STATUS" = 'solved' OR
            "VALUE_STATUS" = 'closed'
        GROUP BY "TICKET_ID"
    ),
    tickets_ordered AS(
        SELECT *,
        row_number() OVER (PARTITION BY "TICKET_ID" ORDER BY "CREATED_AT") as row_number
        FROM tickets
    ),
    tickets_prev_state_duration AS(
        SELECT
            t_cur.*,
            extract(epoch from t_cur."CREATED_AT" - t_prev."CREATED_AT") / 60  as PreviousStateDuration
        FROM tickets_ordered t_cur
        LEFT JOIN tickets_ordered t_prev
        ON t_prev."TICKET_ID" = t_cur."TICKET_ID"
        AND t_prev."row_number" = t_cur."row_number" - 1
    )
SELECT
    tcr."TICKET_ID",
    tcr."creation_time",
    tcl."closing_time",
    extract(epoch from tcl."closing_time" - tcr."creation_time") / 60 as total_time
FROM tickets_creation tcr
LEFT JOIN tickets_closing tcl
ON tcr."TICKET_ID"=tcl."TICKET_ID";

-- WITH 
--     tickets_ordered AS(
--         SELECT *,
--         row_number() OVER (PARTITION BY "TICKET_ID" ORDER BY "CREATED_AT") as row_number
--         FROM tickets
--     )
--     -- tickets_prev_state_duration AS(
-- SELECT
--     t2.*,
--     t1."CREATED_AT" as PreviousStateTime
-- FROM tickets_ordered t1
-- RIGHT JOIN tickets_ordered t2
-- ON t1."TICKET_ID" = t2."TICKET_ID"
-- AND t1."row_number" = t2."row_number" - 1
-- LIMIT 10000
--     -- )