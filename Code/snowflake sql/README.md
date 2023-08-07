# Featured Code

### Snowflake auto-ingest from ADLS


```
-- Create a stage to store the data files.
CREATE OR REPLACE STAGE stage
LOCATION 'abfss://my-container';

-- Create a table to store the loaded data.
CREATE TABLE my_table (
  id INT,
  fname VARCHAR(255),
  lname VARCHAR(255)
  data VARCHAR(255)
);


CREATE OR REPLACE PIPE ingest_pipe
COPY INTO my_table
FROM @stage
FILE_PATTERN='*.csv'
ON_ERROR = SKIP_FILE;

-- Enable auto-ingestion for the pipe.
ALTER PIPE ingest_pipe 
AUTO_INGEST = TRUE;
