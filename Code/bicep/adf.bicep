// =========== adf.bicep ===========

param factoryName string
param factoryLocation string
param pipelineName string

resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: factoryLocation
}

resource pipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: adf
  name: pipelineName
  properties: {
    activities: []
  }
}
