-- CREATE TEMP TABLE test_table AS (
--     SELECT * FROM tickets
-- );
SELECT count(*) AS nb_records
FROM tickets;
SELECT count("TICKET_ID") AS nb_tickets
from (
    SELECT
    "TICKET_ID", count(*)
    from tickets
    GROUP by "TICKET_ID"
);