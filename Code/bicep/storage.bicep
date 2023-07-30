// =========== storage.bicep ===========

param storageAccountName string

/*
  Storage account:
  ZRS Chosen for higher availability at the sacrifice of write latency.
  For data that we want faster delivery we will use Snowflake.
*/

resource stg 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageAccountName
  location: resourceGroup().location
  sku: {
    name: 'Premium_ZRS'
  }
  kind: 'StorageV2'
  tags:{
    Environment : 'dev'
  }
}
