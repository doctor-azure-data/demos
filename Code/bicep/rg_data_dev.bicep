// rg's must be deployed at the tenant level
targetScope = 'subscription'



resource rg_data_dev 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: ''
  location: 'eastus2'
  tags: {
    environment: 'dev'
    team : 'data science'

  }
}
