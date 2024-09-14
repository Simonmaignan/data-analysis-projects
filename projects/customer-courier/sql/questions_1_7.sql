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
    from_id,
    CASE from_id
        WHEN customer_id THEN 'customer'
        ELSE 'courier'
    END AS sender
FROM
    customer_courier_chat_messages
LIMIT
    1;

-- 4. Show the number of messages sent by customer and order_id
SELECT
    customer_id,
    order_id,
    count(*) AS nb_messages
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
    -- Calculate the time difference in seconds (using strftime to calculate the difference)
SELECT
    i.order_id,
    (
        strftime('%s', r.first_response_time) - strftime('%s', i.first_message_time)
    ) AS response_time_seconds
FROM
    initial_message i
    JOIN response_message r ON i.order_id = r.order_id;
