param sourceConnectionString string
param targetDataLakePath string
param targetDataWarehouse string

resource pipeline 'Microsoft.DevOps/pipelines@2021-01-01-preview' = {
  name: 'MedallionDataPipeline'
  location: 'East US'
  properties: {
    configuration: {
      variables: {}
      resources: []
      triggers: []
      pr: {
        branches: {}
      }
      environments: []
      pool: {
        name: 'Azure Pipelines Example'
      }
      steps: [
        {
          task: 'AzureBlobFileCopy@4'
          displayName: 'Copy Data to Data Lake'
          inputs: {
            SourcePath: 'your-source-path'
            azureSubscription: 'your-azure-subscription'
            DestinationContainer: 'your-destination-container'
            DestinationBlob: 'your-destination-blob'
          }
        }
        {
          // ETL Steps here.
        }
      ]
    }
  }
}

output pipelineId string = pipeline.name
