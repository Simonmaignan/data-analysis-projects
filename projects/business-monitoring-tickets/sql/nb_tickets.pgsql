-- CREATE TEMP TABLE test_table AS (
--     SELECT * FROM tickets
-- );
select count("TICKET_ID") from (
    SELECT
    "TICKET_ID", count(*)
    from tickets
    GROUP by "TICKET_ID"
)