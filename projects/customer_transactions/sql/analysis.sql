USE customer_transactions;
-- What is the average transaction value?
SELECT 
    ROUND(AVG(Amount_Spent), 2) AS Avg_Transaction_Value
FROM
    transactions
;
-- What is the total revenue per sector?
SELECT
    Sector,
    ROUND(SUM(Amount_Spent), 2) as Total_Revenue
FROM transactions
GROUP BY Sector
;
-- What is the average spend per visit?
WITH amount_by_visit AS
(
    SELECT
        Customer_ID,
        Transaction_Date,
        SUM(Amount_Spent) AS Total_Spent
    FROM transactions
    GROUP BY Customer_ID, Transaction_Date
)
SELECT ROUND(AVG(Total_Spent), 2) AS Avg_Spend_By_Visit
FROM amount_by_visit
;
-- What are the top 5 sectors with highest average spend per visit?
WITH amount_by_visit AS
(
    SELECT
        Customer_ID,
        Transaction_Date,
        Sector,
        SUM(Amount_Spent) AS Total_Spent
    FROM transactions
    GROUP BY Customer_ID, Transaction_Date, Sector
)
SELECT 
    Sector,
    ROUND(AVG(Total_Spent), 2) AS Avg_Spend_By_Visit
FROM amount_by_visit
GROUP BY Sector
ORDER BY Avg_Spend_By_Visit DESC
LIMIT 5
;
-- Which country has the highest sales?
SELECT 
    Country as Country_Highest_Sales,
    ROUND(SUM(Amount_Spent), 2) as Sales
FROM transactions
GROUP BY Country
ORDER BY Sales DESC
LIMIT 1
;