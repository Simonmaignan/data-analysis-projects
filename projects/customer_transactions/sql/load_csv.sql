CREATE DATABASE customer_transactions;
CREATE TABLE transactions(
    customer_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    brand_name VARCHAR(255) NOT NULL,
    sector VARCHAR(255) NOT NULL,
    gender CHAR,
    date_of_birth DATE,
    country VARCHAR(255),
    no_of_scans INT NOT NULL,
    amount_spent FLOAT NOT NULL
);
LOAD DATA LOCAL INFILE 'dataset/Sample_Dataset.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;