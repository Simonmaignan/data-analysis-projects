-- database: ../dataset/customer-courier.db
-- 3 Show the first message (row) sender (courier or customer)
DROP TABLE IF EXISTS customer_courier_conversations;

CREATE TABLE
    customer_courier_conversations AS
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
    count(*) FILTER (
        WHERE
            c.from_id = c.customer_id
    ) as num_messages_customer,
    min(c.message_sent_time) as conversation_started_at,
    max(c.message_sent_time) as last_message_time
FROM
    customer_courier_chat_messages c
    LEFT JOIN orders o ON c.order_id = o.order_id
GROUP BY
    c.order_id;
