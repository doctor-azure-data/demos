param location string
param name string

resource workspaceName_resource 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: name
  location: location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', name)
    parameters: {
      enableNoPublicIp: {
        value: false
      }
    }
  }
}
