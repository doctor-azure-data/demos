/*
Bicep file build out a template datalake with:
1. Databricks
2. Datafactory with pipeline and datasets for parquet files
3. ADLS2 with Bronze, Silver, Gold layout

*/

param storageAccountName string
param dataFactoryName string
param databricksWorkspaceName string
param adlsDirectoryName string = 'DataLake'
param adlsDirectoryNameBronze string = 'Bronze'
param adlsDirectoryNameSilver string = 'Silver'
param adlsDirectoryNameGold string = 'Gold'

resource adlsAccount 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageAccountName
  location: resourceGroup().location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  identity: {
    type: 'SystemAssigned'
  }
}

resource adlsDirectoryBronze 'Microsoft.Storage/storageAccounts/fileServices/shares/directories@2021-08-01' = {
  name: '${adlsAccount.name}/default/${adlsDirectoryName}/${adlsDirectoryNameBronze}'
}

resource adlsDirectorySilver 'Microsoft.Storage/storageAccounts/fileServices/shares/directories@2021-08-01' = {
  name: '${adlsAccount.name}/default/${adlsDirectoryName}/${adlsDirectoryNameSilver}'
}

resource adlsDirectoryGold 'Microsoft.Storage/storageAccounts/fileServices/shares/directories@2021-08-01' = {
  name: '${adlsAccount.name}/default/${adlsDirectoryName}/${adlsDirectoryNameGold}'
}

resource dataFactory 'Microsoft.DataFactory/factories@2021-06-01' = {
  name: dataFactoryName
  location: resourceGroup().location
  identity: {
    type: 'SystemAssigned'
  }
}

resource dataFactoryDataset 'Microsoft.DataFactory/datasets@2021-06-01' = {
  name: '${dataFactory.name}ProcessPatientsDataset'
  properties: {
    type: 'AzureBlob'
    linkedServiceName: {
      referenceName: 'AzureBlobStorageLinkedService'
      type: 'LinkedServiceReference'
    }
    typeProperties: {
      fileName: 'patient_data.parquet'
      folderPath: 'bronze/'
    }
  }
}

// Modify as needed. Default is Copy Activity
resource dataFactoryPipeline 'Microsoft.DataFactory/pipelines@2021-06-01' = {
  name: '${dataFactory.name}ProcessPatientsPipeline'
  properties: {
    activities: [
      {
        name: 'CopyData'
        type: 'Copy'
        inputs: [
          {
            referenceName: 'SourceDataset'
            type: 'DatasetReference'
          }
        ]
        outputs: [
          {
            referenceName: 'SinkDataset'
            type: 'DatasetReference'
          }
        ]
        typeProperties: {
          source: {
            type: 'DelimitedTextSource'
          }
          sink: {
            type: 'DelimitedTextSink'
          }
          copyBehavior: 'PreserveHierarchy'
        }
      }
    ]
  }
}

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2021-05-01' = {
  name: databricksWorkspaceName
  location: resourceGroup().location
  sku: {
    name: 'standard'
  }
  identity: {
    type: 'SystemAssigned'
  }
}

