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
            extract(epoch from t_cur."CREATED_AT" - t_prev."CREATED_AT") / 60  as "Previous_State_Duration"
        FROM tickets_ordered t_cur
        LEFT JOIN tickets_ordered t_prev
        ON t_prev."TICKET_ID" = t_cur."TICKET_ID"
        AND t_prev."row_number" = t_cur."row_number" - 1
    )
SELECT "TICKET_ID",
    "VALUE_STATUS",
    "VALUE_PREVIOUS_VALUE",
    "Previous_State_Duration" as "FRT"
FROM tickets_prev_state_duration
WHERE "VALUE_PREVIOUS_VALUE" = 'new'
-- SELECT * FROM tickets_prev_state_duration
-- LIMIT 1000