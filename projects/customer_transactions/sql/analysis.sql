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
-- At what age do the customers spend the most on average?
WITH spend_age AS (
    SELECT
        Amount_Spent,
        TIMESTAMPDIFF(YEAR, Date_of_Birth, Transaction_Date) AS Age
FROM transactions)
SELECT
    Age,
    ROUND(AVG(Amount_Spent), 2) AS Avg_Spent
FROM spend_age
GROUP BY Age
ORDER BY Avg_Spent DESC
LIMIT 5
;
-- What is the brand with the highest revenue in each sector?
WITH sector_brand_revenues AS (
    SELECT
        Sector,
        Brand_Name,
        SUM(Amount_Spent) as Revenues
    FROM transactions
    GROUP BY Sector, Brand_Name
), sector_brand_revenues_rank AS
(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Sector
            ORDER BY Revenues DESC
        ) as Revenues_Rank
    FROM sector_brand_revenues
)
SELECT
    Sector,
    Brand_Name,
    ROUND(Revenues, 2) AS Revenues
FROM sector_brand_revenues_rank
WHERE Revenues_Rank = 1
ORDER BY Revenues DESC
;