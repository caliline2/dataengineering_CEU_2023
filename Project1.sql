-- *********************************** --
-- ** Project 1 Data Engineering 23 ** --
-- **        Jana Hochel            ** --
-- *********************************** --

/*
Project: Loans Distributed by the Social Development Bank of Saudi Arabia in 2022

The Social Development Bank is one of the cornerstones of the government of the Kingdom of Saudi Arabia. 
The bank issues interest-free loans for citizens in order to drive development and welfare.
Unlike in other countries, the scope of these loans is broad and largely promote local values.

Target groups:
* SMEs, employers, freelancers and emerging trades,
* Citizens with limited incomes to overcome financial difficulties,
* MSME sector,
* Supporting important life events such as marriage and family,
* Promoting savings among individuals and institutions.
The Bank has 26 branches in different regions.

The goal of analytics is to assess the distribution of these risk-free loans among the population.
One data entry represents one successful application.
The individual debtors have been anonymized. 
Thus, we do not know whether anyone succeeded in claiming financing multiple times.

The fact analysed is the total sum of a loan.

Data source: 
Loans of KSA development banks:https://od.data.gov.sa/Data/en/dataset/social-development-bank-loan-for-2022
Location and population: https://simplemaps.com/data/sa-cities
Cost of living in cities: 
a) https://od.data.gov.sa/Data/en/dataset/average-prices-of-goods-and-services-in-sixteen-cities
b) https://od.data.gov.sa/Data/en/dataset/average-prices-of-goods-and-services-in-sixteen-cities--2-
Weather: https://en.climate-data.org/asia/saudi-arabia/asir-region-1999/

*/
-- OPERATIONAL LAYER: 
-- Step 1: Create schema
CREATE SCHEMA P1_saudi;
USE P1_saudi;


-- Step 2: Create Tables and load data
-- review folder and set permissions
-- SHOW VARIABLES LIKE "secure_file_priv";
-- SHOW VARIABLES LIKE "local_infile";
-- SET GLOBAL local_infile = 1;
-- SHOW VARIABLES LIKE 'sql_mode';
SET SQL_SAFE_UPDATES = 0;

DROP TABLE IF EXISTS cities;
CREATE TABLE cities (
    City TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    country TEXT,
    iso2 TEXT,
    population INT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.1/Uploads/cities.csv' INTO TABLE cities
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS;

DROP TABLE IF EXISTS cost_of_living;
CREATE TABLE cost_of_living (
    City TEXT,
    Furnished_apartment DOUBLE
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.1/Uploads/cost_of_living.csv' INTO TABLE cost_of_living
FIELDS TERMINATED BY ',' -- Change this if your CSV uses a different delimiter
LINES TERMINATED BY '\r\n' -- Change this if your CSV uses a different line ending
IGNORE 1 ROWS; -- Skip the first row containing column names

DROP TABLE IF EXISTS loans;
CREATE TABLE loans (
    City TEXT,
    Classification TEXT,
    Product TEXT,
    Sector TEXT,
    Amount DECIMAL(10, 2),
    Installments TEXT,
    Date TEXT,
    Gender TEXT,
    Age TEXT,
    Status TEXT,
    Special_needs TEXT,
    Provide_Savings Double,
    Value DECIMAL(10, 2)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.1/Uploads/loans.csv' INTO TABLE loans
FIELDS TERMINATED BY ',' -- Change this if your CSV uses a different delimiter
LINES TERMINATED BY '\r\n' -- Change this if your CSV uses a different line ending
IGNORE 1 ROWS; -- Skip the first row containing column names

DROP TABLE IF EXISTS weather;
CREATE TABLE weather (
    ď»żCity TEXT,
    Date TEXT,
    av_sun_hours Double,
    av_temp Double,
    humidity Double,
    max_temp Double,
    precipitation_mm Double
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.1/Uploads/weather.csv' INTO TABLE weather
FIELDS TERMINATED BY ',' -- Change this if your CSV uses a different delimiter
LINES TERMINATED BY '\r\n' -- Change this if your CSV uses a different line ending
IGNORE 1 ROWS; -- Skip the first row containing column names

ALTER TABLE weather
CHANGE COLUMN `ď»żCity` City VARCHAR(255); 
Select * from weather;

/*####################################################################################*
Analytical LAYER:

Step 1: Double check whether everything has been loaded*/
SHOW TABLES;

SELECT TABLE_NAME, GROUP_CONCAT(COLUMN_NAME) AS Header_Names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'p1_saudi'
GROUP BY TABLE_NAME;

DESCRIBE loans;

-- Step 2: Create a copy of data to prevent data loss
DROP TABLE IF EXISTS loans_preview;

CREATE TABLE loans_preview AS SELECT * FROM loans LIMIT 52000;

-- Step 3: Create a denormalized data structure (1 big table)
SELECT *
FROM loans_preview
LEFT JOIN cost_of_living ON loans_preview.City = cost_of_living.City
LEFT JOIN cities ON loans_preview.City = cities.city
LEFT JOIN weather ON loans_preview.City = weather.City AND loans_preview.Date = weather.Date;


-- Step 4: Verify data quality

-- Double check count of cities
SELECT COUNT(DISTINCT City) FROM loans_preview;

-- Descriptive statistics
SELECT
    city,
    ROUND(AVG(Amount)) AS AVG,
    MIN(Amount) AS Min,
    MAX(Amount) AS Max,
    ROUND(STDDEV(Amount)) AS StdDev,
    COUNT(Amount) AS Count
FROM loans_preview
GROUP BY city
ORDER BY city;


/* -- ####################################################################################
-- ETL PIPLINE: Create an ETL pipeline using Stored procedures. 
*/

-- ETL Procedure to generate table with descriptive statistics in 1 procedure
DELIMITER //
DROP PROCEDURE IF EXISTS etl_descriptive;

CREATE PROCEDURE etl_descriptive()
BEGIN
    DROP TABLE IF EXISTS Descriptive;
    CREATE TABLE IF NOT EXISTS Descriptive (
        city VARCHAR(255),
        AVG DECIMAL(10, 2),
        Min DECIMAL(10, 2),
        Max DECIMAL(10, 2),
        StdDev DECIMAL(10, 2),
        Count INT,
        PRIMARY KEY (city)
    );

    -- Extract Data
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_data AS
    SELECT 
        loans_preview.City,
        ROUND(AVG(loans_preview.Amount), 2) AS AVG,
        MIN(loans_preview.Amount) AS Min,
        MAX(loans_preview.Amount) AS Max,
        ROUND(STDDEV(loans_preview.Amount), 2) AS StdDev,
        COUNT(loans_preview.Amount) AS Count
    FROM loans_preview
    LEFT JOIN cost_of_living ON loans_preview.City = cost_of_living.City
    LEFT JOIN cities ON loans_preview.City = cities.city
    LEFT JOIN weather ON loans_preview.City = weather.City AND loans_preview.Date = weather.Date
    GROUP BY loans_preview.City
    ORDER BY loans_preview.City;

    -- Load Data
    INSERT INTO Descriptive SELECT * FROM temp_data;

    -- Clean up temporary table
    DROP TEMPORARY TABLE IF EXISTS temp_data;
END //

DELIMITER ;

CALL etl_descriptive();

select * from descriptive;



-- ETL Procedure to transform metric to imperial system
DELIMITER //

-- Drop the procedure if it exists
DROP PROCEDURE IF EXISTS etl_imperial;

CREATE PROCEDURE etl_imperial()
BEGIN
    -- Drop the imperial table if it exists
    DROP TABLE IF EXISTS imperial;

    -- Drop temp table if it exists
    DROP TEMPORARY TABLE IF EXISTS temp_table;

    -- Extract and Join
    CREATE TEMPORARY TABLE temp_table
    AS
    SELECT loans_preview.*, cost_of_living.Furnished_apartment, cities.country, cities.iso2, cities.latitude, cities.longitude, cities.population, weather.av_sun_hours, weather.av_temp
    FROM loans_preview
    LEFT JOIN cost_of_living ON loans_preview.City = cost_of_living.City
    LEFT JOIN cities ON loans_preview.City = cities.city
    LEFT JOIN weather ON loans_preview.City = weather.City AND loans_preview.Date = weather.Date;

    -- Add av_temp_fahrenheit column
    ALTER TABLE temp_table ADD COLUMN av_temp_fahrenheit DECIMAL(10,2);

    -- Transform
    UPDATE temp_table
    SET av_temp_fahrenheit = (av_temp * 9/5) + 32
    WHERE av_temp IS NOT NULL;

    -- Load into Permanent Table
    CREATE TABLE imperial
    AS
    SELECT City, country, iso2, latitude, longitude, population, Furnished_apartment, Age, Amount, Classification, Date, Gender, Installments, Product, Provide_Savings, Sector, Special_needs, Status, Value, av_sun_hours, av_temp, av_temp_fahrenheit
    FROM temp_table;
END //

DELIMITER ;

-- Call the Procedure
CALL etl_imperial();

select av_temp_fahrenheit from imperial;



-- ####################################################################################
-- DATA MART: Create Views as data marts.

-- Data mart to view on a map average amount of loans in each city
DROP VIEW IF EXISTS descriptive_datamart;

CREATE VIEW descriptive_datamart AS
SELECT
    Descriptive.*,
    cities.latitude,
    cities.longitude
FROM Descriptive
JOIN cities ON Descriptive.city = cities.City;

select * from descriptive_datamart;


-- Datamart for total amount by sector
DROP VIEW IF EXISTS LoanAmountBySector;

CREATE VIEW LoanAmountBySector AS
SELECT Sector, SUM(Amount) AS TotalLoanAmount
FROM loans_preview
GROUP BY Sector;

Select * from LoanAmountBySector;

-- Datamart for amount by sector by age - older men seem to receive more than older women
-- This may be because I have filtered for business and projects loans and older women had higher barriers to entry than women nowadays
DROP VIEW IF EXISTS AgeGroupAnalysis;
CREATE VIEW AgeGroupAnalysis AS
SELECT
    Age AS AgeGroup,
    Gender,
    COUNT(*) AS LoansCount,
    ROUND(AVG(Amount), 2) AS AvgAmount
FROM loans_preview
GROUP BY Age, Gender;

SELECT * FROM AgeGroupAnalysis;


/* -- ####################################################################################
-- ETL PIPLINE: Create an ETL pipeline using Triggers.
*/

-- Debugging/Logging + Trigger

CREATE TABLE IF NOT EXISTS loans_audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    action_performed VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER //

CREATE TRIGGER on_loans_preview_update
AFTER UPDATE ON loans_preview
FOR EACH ROW
BEGIN
    INSERT INTO loans_audit_log (action_performed)
    VALUES ('An update occurred in loans_preview');
END;
//

DELIMITER ;











