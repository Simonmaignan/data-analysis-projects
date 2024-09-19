WITH
    tickets_creation AS(
        SELECT
            "TICKET_ID",
            min("CREATED_AT") as creation_time
        from tickets
        WHERE "VALUE_STATUS" = 'new'
        GROUP BY "TICKET_ID"
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
        row_number() OVER (PARTITION BY "TICKET_ID" ORDER BY "CREATED_AT") as "Row_Number"
        FROM tickets
    ),
    tickets_prev_state_duration AS(
        SELECT
            t_cur.*,
            extract(epoch from t_cur."CREATED_AT" - t_prev."CREATED_AT") / 60  as "Previous_State_Duration"
        FROM tickets_ordered t_cur
        LEFT JOIN tickets_ordered t_prev
        ON t_prev."TICKET_ID" = t_cur."TICKET_ID"
        AND t_prev."Row_Number" = t_cur."Row_Number" - 1
    ),
    tickets_waiting_time AS(
        SELECT "TICKET_ID",
            sum("Previous_State_Duration") FILTER (
                WHERE
                    "VALUE_PREVIOUS_VALUE" = ANY(ARRAY['new', 'open', 'hold']) 
            ) as "Waiting_Time"
        FROM tickets_prev_state_duration
        GROUP BY "TICKET_ID"
    ),
    tickets_frp AS(
        SELECT "TICKET_ID",
            "Previous_State_Duration" as "FRT"
        FROM tickets_prev_state_duration
        WHERE "VALUE_PREVIOUS_VALUE" = 'new'
    ),
    tickets_times AS(
        SELECT
            tcr."TICKET_ID",
            tcl."closing_time"::date as "Solved_Date",
            extract(epoch from tcl."closing_time" - tcr."creation_time") / 60 as "Total_Time",
            tw."Waiting_Time",
            tfrp."FRT"
        FROM tickets_creation tcr
        LEFT JOIN tickets_closing tcl ON tcr."TICKET_ID"=tcl."TICKET_ID"
        LEFT JOIN tickets_waiting_time tw ON tcr."TICKET_ID"=tw."TICKET_ID"
        LEFT JOIN tickets_frp tfrp ON tcr."TICKET_ID"=tfrp."TICKET_ID"
    )
SELECT "Solved_Date",
    avg("Total_Time") FILTER (
        WHERE
            "Total_Time" >= 1 
    ) as "AVG_Total_Time",
    avg("Waiting_Time") FILTER (
        WHERE
            "Waiting_Time" >= 1 
    ) as "AVG_Waiting_Time",
    avg("FRT") FILTER (
        WHERE
            "FRT" >= 1 
    ) as "AVG_FRT"
FROM tickets_times
GROUP BY "Solved_Date"
ORDER BY "Solved_Date"
