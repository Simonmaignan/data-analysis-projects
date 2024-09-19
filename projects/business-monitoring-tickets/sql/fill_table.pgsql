WITH tickets_statuses AS (
    SELECT
        "TICKET_ID",
        array_agg("VALUE_STATUS") as statuses
    FROM tickets
    GROUP BY "TICKET_ID"
),
tickets_no_creation AS(
    SELECT *
    FROM tickets_statuses
    WHERE NOT ('new' = ANY ("statuses"))
),
tickets_not_solved AS(
    SELECT *
    FROM tickets_statuses
    WHERE NOT ('solved' = ANY ("statuses") OR 'closed' = ANY ("statuses"))
)
INSERT INTO tickets
SELECT
    "TICKET_ID",
    'new',
    NULL,
    -1,
    '2024-04-14 13:00:00'::timestamp
FROM tickets_no_creation
UNION
SELECT
    "TICKET_ID",
    'solved',
    NULL,
    -1,
    '2024-05-01 13:00:00'::timestamp
FROM tickets_not_solved;