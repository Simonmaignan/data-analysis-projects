-- database: ../dataset/customer-courier.db
-- 1. Show all the columns order by order_id
SELECT
    *
FROM
    orders
ORDER BY
    order_id;

-- 2. Show all the columns order by city_code
SELECT
    *
FROM
    orders
ORDER BY
    city_code;

-- 3. Show the first message (row) sender (courier or customer)
SELECT
    customer_id,
    courier_id
FROM
    customer_courier_chat_messages
LIMIT
    1;

-- 4. Show the number of messages sent by customer and order_id
SELECT
    customer_id,
    order_id,
    count(*)
FROM
    customer_courier_chat_messages
GROUP BY
    customer_id,
    order_id;

-- 5. Show the first message (row) in the conversation by order_id
SELECT
    *
FROM
    customer_courier_chat_messages
GROUP BY
    order_id;

--6. Show the last message (row) in the conversation by order_id
WITH
    last_record_cte AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY
                    order_id
                ORDER BY
                    `index` DESC
            ) as RowNumber
        FROM
            customer_courier_chat_messages
    )
SELECT
    *
FROM
    last_record_cte
WHERE
    RowNumber = 1;

-- 7. Show the time (in secs) elapsed until the first message was responded by order_id
WITH
    last_record_cte AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY
                    order_id
                ORDER BY
                    message_sent_time
            ) as RowNumber
        FROM
            customer_courier_chat_messages
    )
SELECT
    *
FROM
    last_record_cte
WHERE
    RowNumber = 1
