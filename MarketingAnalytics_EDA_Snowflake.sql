--- Dimension Tables ---
/* Query to categorize products based on their price */

SELECT 
    ProductID,  
    ProductName, 
    Price,  
	
    CASE -- Categorizes the products into price categories: Low, Medium, or High
        WHEN Price < 50 THEN 'Low'  
        WHEN Price BETWEEN 50 AND 200 THEN 'Medium'  
        ELSE 'High'  
    END AS PriceCategory  -- Names the new column as PriceCategory

FROM 
    products;  

/* Query to join dim_customers with dim_geography to enrich customer data with geographic information */

SELECT 
    c.CustomerID,  
    c.CustomerName,  
    c.Email,  
    c.Gender,  
    c.Age,  
    g.Country, 
    g.City  
FROM 
    customers as c 
INNER JOIN
    geography g  
ON 
    c.GeographyID = g.GeographyID; 
    

--- Fact Tables ---

/* Query to clean whitespace issues in the ReviewText column */

SELECT 
    ReviewID,  
    CustomerID,  
    ProductID, 
    ReviewDate,  
    Rating,  -- Selects the numerical rating given by the customer (e.g., 1 to 5 stars)
    -- Cleans up the ReviewText by replacing double spaces with single spaces to ensure the text is more readable and standardized
    REPLACE(ReviewText, '  ', ' ') AS ReviewText
FROM 
    customer_reviews; 

/* Query to clean and normalize the engagement_data table */

SELECT 
    EngagementID, 
    ContentID, 
    CampaignID,
    ProductID,
    UPPER(REPLACE(ContentType, 'Socialmedia', 'Social Media')) AS ContentType,  
    SUBSTRING(ViewsClicksCombined, 1, POSITION('-' IN ViewsClicksCombined) - 1) AS Views,  
    SUBSTRING(ViewsClicksCombined, POSITION('-' IN ViewsClicksCombined) + 1) AS Clicks,  
    Likes,  
    TO_CHAR(EngagementDate, 'DD.MM.YYYY') AS EngagementDate  
FROM 
    engagement_data  
WHERE 
    ContentType != 'Newsletter';  -- Filters out rows where ContentType is 'Newsletter' as these are not relevant for our analysis

/* Common Table Expression (CTE) to identify and tag duplicate records */

WITH DuplicateRecords AS (
    SELECT 
        JourneyID,  
        CustomerID,  
        ProductID,  
        VisitDate,  
        Stage,  
        Action, 
        Duration,  
        -- Use ROW_NUMBER() to assign a unique row number to each record within the partition defined below
        ROW_NUMBER() OVER (
            -- PARTITION BY groups the rows based on the specified columns that should be unique
            PARTITION BY CustomerID, ProductID, VisitDate, Stage, Action  
            -- ORDER BY defines how to order the rows within each partition (usually by a unique identifier like JourneyID)
            ORDER BY JourneyID  
        ) AS row_num  -- This creates a new column 'row_num' that numbers each row within its partition
    FROM 
        customer_journey 
)

-- Select all records from the CTE where row_num > 1, which indicates duplicate entries
    
SELECT *
FROM DuplicateRecords
WHERE row_num > 1  -- Filters out the first occurrence (row_num = 1) and only shows the duplicates (row_num > 1)
ORDER BY JourneyID

/* Outer query selects the final cleaned and standardized data */
    
SELECT 
    JourneyID,  
    CustomerID,  
    ProductID,  
    VisitDate,  
    Stage,  
    Action, 
    COALESCE(Duration, avg_duration) AS Duration  -- Replaces missing durations with the average duration for the corresponding date
FROM 
    (
        -- Subquery to process and clean the data
        SELECT 
            JourneyID,  
            CustomerID, 
            ProductID,  
            VisitDate,  
            UPPER(Stage) AS Stage,  -- Converts Stage values to uppercase for consistency in data analysis
            Action,  
            Duration, 
            AVG(Duration) OVER (PARTITION BY VisitDate) AS avg_duration,  -- Calculates the average duration for each date, using only numeric values
            ROW_NUMBER() OVER (
                PARTITION BY CustomerID, ProductID, VisitDate, UPPER(Stage), Action  -- Groups by these columns to identify duplicate records
                ORDER BY JourneyID  -- Orders by JourneyID to keep the first occurrence of each duplicate
            ) AS row_num  -- Assigns a row number to each row within the partition to identify duplicates
        FROM 
            customer_journey  -- Specifies the source table from which to select the data
    ) AS subquery  -- Names the subquery for reference in the outer query
WHERE 
    row_num = 1;  -- Keeps only the first occurrence of each duplicate group identified in the subquery    
    