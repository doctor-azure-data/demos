# Example Data Factory template for migrating Oracle to Azure

--- 

```
param adfName string
param oracleServerName string
param oracleDatabaseName string
param oracleUserName string
param oraclePassword string
param azureSqlServerName string
param azureSqlDatabaseName string
param azureSqlUserName string
param azureSqlPassword string

resource adfResource 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: adfName
  location: resourceGroup().location
  identity: {
    type: 'SystemAssigned'
  }
}

resource linkedServiceOracle 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  name: '${adfResource.name}/oracleLinkedService'
  properties: {
    type: 'Oracle'
    typeProperties: {
      connectionString: 'Data Source=${oracleServerName};Initial Catalog=${oracleDatabaseName};User ID=${oracleUserName};Password=${oraclePassword}'
    }
  }
}

resource linkedServiceAzureSql 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  name: '${adfResource.name}/azureSqlLinkedService'
  properties: {
    type: 'AzureSqlDatabase'
    typeProperties: {
      connectionString: 'Data Source=tcp:${azureSqlServerName}.database.windows.net,1433;Initial Catalog=${azureSqlDatabaseName};User ID=${azureSqlUserName};Password=${azureSqlPassword};Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;'
    }
  }
}

resource oracleDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${adfResource.name}/oracleDataset'
  properties: {
    type: 'Oracle'
    linkedServiceName: linkedServiceOracle
    typeProperties: {
      tableName: 'table_name'
    }
  }
}

resource azureSqlDataset 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  name: '${adfResource.name}/azureSqlDataset'
  properties: {
    type: 'AzureSqlTable'
    linkedServiceName: linkedServiceAzureSql
    typeProperties: {
      tableName: 'azure_sql_table'
    }
  }
}

resource copyActivity 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  name: '${adfResource.name}/copyDataFromOracleToAzureSqlPipeline'
  properties: {
    activities: [
      {
        name: 'CopyDataFromOracleToAzureSql',
        type: 'Copy',
        inputs: [
          {
            name: oracleDataset
          }
        ],
        outputs: [
          {
            name: azureSqlDataset
          }
        ],
       

