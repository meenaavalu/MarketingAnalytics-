----- Creating Integration Object ------
CREATE OR REPLACE STORAGE INTEGRATION S3_Integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'AWS_ROLE_ARN' -- AWS_ROLE_ARN
STORAGE_ALLOWED_LOCATIONS = ('path') --s3 bucket location
COMMENT = 'Optional Comment';

//description Integration Object
desc INTEGRATION S3_Integration;