-- database: ../dataset/customer-courier.db
-- 3 Show the first message (row) sender (courier or customer)
DROP TABLE IF EXISTS customer_courier_conversations;

CREATE TABLE
    customer_courier_conversations AS
WITH
    initial_message AS (
        -- Get the first message (i.e., earliest message_sent_time) for each order_id
        SELECT
            order_id,
            from_id AS initiator_id,
            MIN(message_sent_time) AS first_message_time
        FROM
            customer_courier_chat_messages
        GROUP BY
            order_id
    ),
    last_message AS (
        -- Get the last message (i.e., latest message_sent_time) for each order_id
        SELECT
            order_id,
            MAX(message_sent_time) AS last_message_time,
            order_stage as last_order_stage
        FROM
            customer_courier_chat_messages
        GROUP BY
            order_id
    ),
    response_message AS (
        -- Get the first response message for the same order, but from a different sender
        SELECT
            c.order_id,
            MIN(c.message_sent_time) AS first_response_time
        FROM
            customer_courier_chat_messages c
            JOIN initial_message i ON c.order_id = i.order_id
            AND c.from_id != i.initiator_id -- Ensure it's a response from a different sender
        GROUP BY
            c.order_id
    )
SELECT
    c.order_id as order_id,
    o.city_code as city_code,
    min(c.message_sent_time) FILTER (
        WHERE
            c.from_id = c.courier_id
    ) as first_courier_message,
    min(c.message_sent_time) FILTER (
        WHERE
            c.from_id = c.customer_id
    ) as first_customer_message,
    count(*) FILTER (
        WHERE
            c.from_id = c.courier_id
    ) as num_messages_courier,
    CASE i.initiator_id
        WHEN c.customer_id THEN 'customer'
        ELSE 'courier'
    END AS first_message_by,
    count(*) FILTER (
        WHERE
            c.from_id = c.customer_id
    ) as num_messages_customer,
    i.first_message_time as conversation_started_at,
    (
        strftime('%s', r.first_response_time) - strftime('%s', i.first_message_time)
    ) AS first_responsetime_delay_seconds,
    l.last_message_time as last_message_time,
    l.last_order_stage as last_message_order_stage
FROM
    customer_courier_chat_messages c
    LEFT JOIN initial_message i on c.order_id = i.order_id
    LEFT JOIN response_message r ON i.order_id = r.order_id
    LEFT JOIN last_message l on i.order_id = l.order_id
    LEFT JOIN orders o ON l.order_id = o.order_id
GROUP BY
    c.order_id;
