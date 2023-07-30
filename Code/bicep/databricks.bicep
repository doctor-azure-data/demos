param location string
param name string

resource db 'Microsoft.Databricks/workspaces@2023-02-01' = {
location: location
name: name

}
