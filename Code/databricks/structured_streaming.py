Databricks Structured Streaming Source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("StructuredStreamingDemo") \
.getOrCreate()

raw_events = spark \
.readStream \
.format("eventhubs") \
.option("eventhubs.connectionString", "YOUR_EVENTHUBS_CONNECTION_STRING") \
.load()

# Start Structured Streaming Job
query = raw_events \
.writeStream \
.outputMode("append") \
.format("memory") \
.queryName("raw_events_stream") \
.start()


# Azure Data Factory Pipeline JSON Definition:
{
"name": "DatabricksToDeltaLakePipeline",
"properties": {
"activities": [
{
"name": "CopyDataFromDatabricksToDeltaLake",
"type": "Copy",
"inputs": [
{
"referenceName": "DatabricksStreamDataset",
"type": "DatasetReference"
}
],
"outputs": [
{
"referenceName": "DeltaLakeDataset",
"type": "DatasetReference"
}
]
}
],
"start": "2023-08-01T00:00:00Z",
"end": "2023-08-02T00:00:00Z",
"isPaused": false,
"hubName": "YOUR_ADF_HUB_NAME",
"pipelineMode": "Scheduled"
}
}
