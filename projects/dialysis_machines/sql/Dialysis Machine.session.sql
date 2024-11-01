WITH machine_data AS(
    SELECT * FROM machine_data_1
    UNION ALL
    SELECT * FROM machine_data_2
)
SELECT 
    date_trunc('day', date) AS day,
    avg(num_treatments)
FROM machine_data
GROUP BY day
LIMIT 5
;

WITH machine_data AS(
    SELECT * FROM machine_data_1
    UNION ALL
    SELECT * FROM machine_data_2
)
SELECT 
    m_data.machine_id,
    m_details.manufacturer,
    m_details.location,
    sum(hours_of_operation) AS total_operating_time
FROM machine_data m_data
LEFT JOIN machine_details m_details ON m_data.machine_id = m_details.machine_id
GROUP BY m_data.machine_id, m_details.manufacturer, m_details.location,
ORDER BY total_operating_time DESC
LIMIT 5
;

WITH machine_data AS(
    SELECT * FROM machine_data_1
    UNION ALL
    SELECT * FROM machine_data_2
),
machine_highest_nb_alarms AS(
    SELECT 
        machine_id,
        count(alarms) AS nb_alarms
    FROM machine_data
    WHERE date BETWEEN (TIMESTAMP '2024-03-01') AND (TIMESTAMP '2024-03-31')
    GROUP BY machine_id
    ORDER BY nb_alarms DESC
    LIMIT 1
)
SELECT *
FROM maintenance_records
WHERE machine_id = (SELECT machine_id FROM machine_highest_nb_alarms) AND
date BETWEEN (TIMESTAMP '2024-03-01') AND (TIMESTAMP '2024-03-31')
;