-- example code for real time data for Snowflake

CREATE DATABASE RealTimeDB;

USE DATABASE RealTimeDB;

CREATE OR REPLACE TABLE RawEvents (
  EventID STRING,
  EventType STRING,
  EventData VARIANT,
  EventTimestamp TIMESTAMP,
  PRIMARY KEY (EventID)
);

CREATE OR REPLACE STREAM RealTimeStream
  ON TABLE RawEvents;

CREATE OR REPLACE TASK ProcessRealTimeTask
  WAREHOUSE = <> 
  SCHEDULE = '1 MINUTE'
  AS
  INSERT INTO ProcessedData
  SELECT
    EventID,
    EventType,
    EventData:field1 AS Field1,
    EventData:field2 AS Field2,
    EventTimestamp
  FROM RealTimeStream;

