-- CREATE TEMP TABLE test_table AS (
--     SELECT * FROM tickets
-- );
WITH tickets_ordered AS(
        SELECT *,
        row_number() OVER (PARTITION BY "TICKET_ID" ORDER BY "CREATED_AT") as row_number
        FROM tickets
    ),
    tickets_prev_state_duration AS(
        SELECT
            t_cur.*,
            extract(epoch from t_cur."CREATED_AT" - t_prev."CREATED_AT") / 60  as previous_state_duration
        FROM tickets_ordered t_cur
        LEFT JOIN tickets_ordered t_prev
        ON t_prev."TICKET_ID" = t_cur."TICKET_ID"
        AND t_prev."row_number" = t_cur."row_number" - 1
    )
-- SELECT "TICKET_ID",
--     sum("previous_state_duration") FILTER (
--         WHERE
--             "VALUE_PREVIOUS_VALUE" = ANY(ARRAY['new', 'open', 'hold']) 
--     ) as first_courier_message
-- FROM tickets_prev_state_duration
-- GROUP BY "TICKET_ID"
SELECT * FROM tickets_prev_state_duration
LIMIT 1000