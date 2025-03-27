/* Creating Database */

CREATE database MarketingAnalytics;

create schema Customer;

/* Creating Tables in Snowflake */

CREATE TABLE customer_journey(
	JourneyID smallint NOT NULL,
	CustomerID tinyint NOT NULL,
	ProductID tinyint NOT NULL,
	VisitDate date NOT NULL,
	Stage nvarchar(50) NOT NULL,
	Action nvarchar(50) NOT NULL,
	Duration float NULL
);

CREATE TABLE customer_reviews(
	ReviewID smallint NOT NULL,
	CustomerID tinyint NOT NULL,
	ProductID tinyint NOT NULL,
	ReviewDate date NOT NULL,
	Rating tinyint NOT NULL,
	ReviewText nvarchar(100) NOT NULL
);

CREATE TABLE customers(
	CustomerID tinyint NOT NULL,
	CustomerName nvarchar(50) NOT NULL,
	Email nvarchar(50) NOT NULL,
	Gender nvarchar(50) NOT NULL,
	Age tinyint NOT NULL,
	GeographyID tinyint NOT NULL
);

CREATE TABLE engagement_data(
	EngagementID smallint NOT NULL,
	ContentID tinyint NOT NULL,
	ContentType nvarchar(50) NOT NULL,
	Likes smallint NOT NULL,
	EngagementDate date NOT NULL,
	CampaignID tinyint NOT NULL,
	ProductID tinyint NOT NULL,
	ViewsClicksCombined nvarchar(50) NOT NULL
);

CREATE TABLE geography(
	GeographyID tinyint NOT NULL,
	Country nvarchar(50) NOT NULL,
	City nvarchar(50) NOT NULL
);

CREATE TABLE products(
	ProductID tinyint NOT NULL,
	ProductName nvarchar(50) NOT NULL,
	Category nvarchar(50) NOT NULL,
	Price float NULL
);

/* Creating a Stage for S3 Bucket */
create stage MarketingAnalytics.Customer.stage
url = 's3 bucket path'
storage_integration = s3_integration

/* Verifying Files in S3 from Snowflake */

LIST @stage;

// copying data from s3 to snowflake 

---- Create a Snowpipe to Automate the Load ---

CREATE PIPE Customer.customer_journey_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.customer_journey
FROM @stage/customer_journey.csv
FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

CREATE PIPE Customer.customers_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.customers
FROM @stage/customers.csv
FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
on_error = 'continue';

CREATE PIPE Customer.engagement_data_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.engagement_data
FROM @stage/engagement_data.csv
FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
on_error = 'continue';

CREATE PIPE Customer.geography_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.geography
FROM @stage/geography.csv
FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
on_error = 'continue';

CREATE PIPE Customer.products_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.products
FROM @stage/products.csv
FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
on_error = 'continue';

CREATE PIPE Customer.customer_reviews_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO Customer.customer_reviews
FROM @stage/customer_reviews.csv
FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
on_error = 'continue';



