/*
  This is example code created to help you evaluate the type of work i've done

  Param file is used to store variables. 
  Upon deployment the file will create a resource group with all required artifacts

  Main bicep file uses modules to create resources.

  The individual module file contians the implementation code
    This makes life easy when new changes down the road come.
*/

targetScope = 'subscription'

param storageAccountName string
param pipelineName string
param dbName string
param synapseName  string
param rgLocation  string
param rgName string

// Creating resource group
resource rg 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: rgName
  location: rgLocation
}

/*
  Deploying storage account using modules
  All data resource are going to be grouped by this pace-layer
*/
module stg './storage.bicep' = {
  name: 'storageDeployment'
  scope: rg    
  params: {
    storageAccountName: storageAccountName
  }
}

// Deploying data factory and its pipeline
module adf './adf.bicep' = {
 name: 'devprocesspatients'
 scope: rg
  params: {
    factoryName: 'devprocesspatients'
    factoryLocation: rg.location
    pipelineName: pipelineName
  }


}

module db 'databricks.bicep' ={
  scope: rg
  name: 'devdatascienceatscale'
  params: {
    location: rg.location
    name: dbName
  }
}

// Some configurations are passed here, while others are defaulted
module synapse 'synapse.bicep' = {
  scope: rg
  name: synapseName
  params:{
    synapseWorkspaceName: ''
    sessionLevelPackagesEnabled: true
    location: rg.location
    sparkAutoPauseDelayInMinutes: 1
    sparkAutoPauseEnabled: false
    sparkAutoScaleEnabled: false
    sparkAutoScaleMaxNodeCount: 1

  }
}
