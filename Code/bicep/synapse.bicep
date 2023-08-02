param sparkPoolPurpose string = 'trainAndExercise'
param synapseWorkspaceName string
param location string = resourceGroup().location
param sparkAutoScaleEnabled bool = true
param sparkAutoScaleMaxNodeCount int = 6

param sparkPoolTags object = {
  department : 'DataScienceAtScale'
  resourceOwner: 'Travis'
}
param sparkAutoScaleMinNodeCount int = 1
param sparkIsolatedComputeEnabled bool = false
param sparkNodeCount int = 0

@allowed([
  'MemoryOptimized'
  'HardwareAccelerated'
])
param sparkNodeSizeFamily string = 'MemoryOptimized'

@allowed([
  'Small'
  'Medium'
  'Large'
  'XLarge'
  'XXLarge'
])
param sparkNodeSize string = 'Medium'
param sparkAutoPauseEnabled bool = true
param sparkAutoPauseDelayInMinutes int = 5
@allowed([
  '3.3'
  '3.2'
  '3.1'
])
param sparkVersion string = '3.3'
param sparkDynamicExecutorEnabled bool = true

@minValue(1)
@maxValue(198)
param sparkMinExecutorCount int = 1

@minValue(2)
@maxValue(199)
param sparkMaxExecutorCount int = 3

@minValue(0)
@maxValue(100)
param sparkCacheSize int = 25

param sparkConfigPropertiesFileName string = ''

param sparkConfigPropertiesContent string = ''

param sessionLevelPackagesEnabled bool = false

// Format the Spark Pool Name (Max length is 15 characters)
var sparkPoolName = 'synsp-${sparkPoolPurpose}}'

// Get the existing Synapse Workspace (Used for Output purposes mainly)
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' existing = {
  name: synapseWorkspaceName
}

// Create Spark Pool Resource
/*resource sparkPool 'Microsoft.Synapse/workspaces/bigDataPools@2021-06-01' = {
  parent: synapseWorkspace
  name: sparkPoolName
  location: location
  tags: sparkPoolTags
  properties: {
    nodeCount: sparkNodeCount
    nodeSizeFamily: sparkNodeSizeFamily
    nodeSize: sparkNodeSize
    autoScale: {
      enabled: sparkAutoScaleEnabled
      minNodeCount: sparkAutoScaleMinNodeCount
      maxNodeCount: sparkAutoScaleMaxNodeCount
    }
    autoPause: {
      enabled: sparkAutoPauseEnabled
      delayInMinutes: sparkAutoPauseDelayInMinutes
    }
    sparkVersion: sparkVersion
    sparkConfigProperties: {
      filename: sparkConfigPropertiesFileName
      content: sparkConfigPropertiesContent
    }
    isComputeIsolationEnabled: sparkIsolatedComputeEnabled
    sessionLevelPackagesEnabled: sessionLevelPackagesEnabled
    dynamicExecutorAllocation: {
      enabled: sparkDynamicExecutorEnabled
      minExecutors: sparkMinExecutorCount
      maxExecutors: sparkMaxExecutorCount
    }

  }
}*/
