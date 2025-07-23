# Create the main resource group
az group create \
  --name rg-stock-sentiment-analysis \
  --location eastus2 \
  --tags Environment=Development Project=StockSentiment
  
  # Create storage account with hierarchical namespace (Data Lake Gen2)
az storage account create \
  --name dlsstocksentiment2025 \
  --resource-group rg-stock-sentiment-analysis \
  --location eastus2 \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --tags Environment=Development Project=StockSentiment


# Get the storage account name
STORAGE_ACCOUNT_NAME="dlsstocksentiment2025"

# Create the three containers for medallion architecture
az storage container create \
  --account-name $STORAGE_ACCOUNT_NAME \
  --name bronze \
  --auth-mode login

az storage container create \
  --account-name $STORAGE_ACCOUNT_NAME \
  --name silver \
  --auth-mode login

az storage container create \
  --account-name $STORAGE_ACCOUNT_NAME \
  --name gold \
  --auth-mode login
  
  
  # Create Event Hubs namespace
az eventhubs namespace create \
  --name eh-stock-sentiment-2025 \
  --resource-group rg-stock-sentiment-analysis \
  --location eastus2 \
  --sku Standard \
  --capacity 2 \
  --enable-auto-inflate true \
  --maximum-throughput-units 4

# Note: Save the namespace name from the output

# Create the event hub
EVENT_HUB_NAMESPACE="eh-stock-sentiment-2025"

az eventhubs eventhub create \
  --name stock-data-hub \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group rg-stock-sentiment-analysis \
  --partition-count 4
  
  
  # Create Databricks workspace
az databricks workspace create \
  --name databricks-stock-sentiment \
  --resource-group rg-stock-sentiment-analysis \
  --location westus2 \
  --sku premium \
  --tags Environment=Development Project=StockSentiment
  
  # List all resources in the resource group to verify
az resource list \
  --resource-group rg-stock-sentiment-analysis \
  --output table
  
  # Get storage account connection string
az storage account show-connection-string \
  --name $STORAGE_ACCOUNT_NAME \
  --resource-group rg-stock-sentiment-analysis \
  --output tsv

# Get Event Hub connection string
az eventhubs namespace authorization-rule keys list \
  --resource-group rg-stock-sentiment-analysis \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString \
  --output tsv
 
 az keyvault set-policy \
  --name kv-stock-sentiment-2025 \
  ---object-id 68d4b1fe-6876-4157-8047-a0e9ea8c11e4 \
  --secret-permissions get list set delete backup restore recover purge
 
 ╰─$ # Assign yourself Key Vault Secrets Officer role                                                                  1 ↵
az role assignment create \
  --assignee 68d4b1fe-6876-4157-8047-a0e9ea8c11e4 \
  --role "Key Vault Secrets Officer" \
  --scope "/subscriptions/$(az account show --query id --output tsv)/resourceGroups/rg-stock-sentiment-analysis/providers/Microsoft.KeyVault/vaults/kv-stock-sentiment-2025"

  
 az keyvault create \
  --name kv-stock-sentiment-2025 \
  --resource-group rg-stock-sentiment-analysis \
  --location eastus2 \
  --sku standard
  
  KEY_VAULT_NAME="kv-stock-sentiment-2025"

# Add Event Hub connection string (replace with your actual connection string)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "event-hub-connection-string" \
  --value "Endpoint=sb://eh-stock-sentiment-2025.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AQyOtR5PpNma9Lu+R/p12zq80dxnvE+RH+AEhIxU1JY="

# Add Storage connection string (replace with your actual connection string)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "storage-connection-string" \
  --value "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=dlsstocksentiment2025;AccountKey=Ob8j34mkPlz8GEO7PLsflGUwQE18gUVazR98SeoZDCGnZGcxjPhC9Hdb1vi+9R5G9/poTQ76eGhv+AStMuDV6w==;BlobEndpoint=https://dlsstocksentiment2025.blob.core.windows.net/;FileEndpoint=https://dlsstocksentiment2025.file.core.windows.net/;QueueEndpoint=https://dlsstocksentiment2025.queue.core.windows.net/;TableEndpoint=https://dlsstocksentiment2025.table.core.windows.net/"
  
   # Get Key Vault details needed for Databricks
az keyvault show \
  --name $KEY_VAULT_NAME \
  --resource-group rg-stock-sentiment-analysis \
  --query "{name:name, resourceId:id, vaultUri:properties.vaultUri}" \
  --output table
  
  # Get your subscription ID
SUBSCRIPTION_ID=$(az account show --query id --output tsv)

# Create service principal with Key Vault access
az ad sp create-for-rbac \
  --name "sp-databricks-keyvault" \
  --role "Key Vault Secrets User" \
  --scopes "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/rg-stock-sentiment-analysis/providers/Microsoft.KeyVault/vaults/$KEY_VAULT_NAME"
  
  az ad sp show --id "a01a4407-9d85-41bb-9005-d1d491ab0cf7" --query id --output tsv
SP_OBJECT_ID="9a543237-6fa6-4b74-a48a-44ab66f9c8a6"
KEY_VAULT_NAME="kv-stock-sentiment-2025"
az keyvault set-policy \
  --name $KEY_VAULT_NAME \
  --object-id $SP_OBJECT_ID \
  --secret-permissions get list
  
  # Get Databricks workspace details                                                                                1 ↵
az databricks workspace show \
  --name databricks-stock-sentiment-canada \
  --resource-group rg-stock-sentiment-analysis \
  --query "{name:name, id:id, managedResourceGroupId:managedResourceGroupId}" \
  --output table
  
 az role assignment create \
  --assignee 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/39874c3d-c294-4750-8e03-daa09da31a3b/resourceGroups/rg-stock-sentiment-analysis/providers/Microsoft.KeyVault/vaults/kv-stock-sentiment-2025"
  
  # Get your Databricks workspace object ID
DATABRICKS_IDENTITY=$(az ad sp list --display-name "databricks-stock-sentiment-canada" --query "[0].id" --output tsv)

# If the above doesn't work, try this alternative
az role assignment create \
  --assignee-object-id "68d4b1fe-6876-4157-8047-a0e9ea8c11e4" \
  --assignee-principal-type ServicePrincipal \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/39874c3d-c294-4750-8e03-daa09da31a3b/resourceGroups/rg-stock-sentiment-analysis/providers/Microsoft.KeyVault/vaults/kv-stock-sentiment-2025"
  
    az role assignment create \
  --assignee 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/39874c3d-c294-4750-8e03-daa09da31a3b/resourceGroups/rg-stock-sentiment-analysis/providers/Microsoft.KeyVault/vaults/kv-stock-sentiment-2025"

# Add Polygon API key to Key Vault
az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "polygon-api-key" \
  --value "Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q"

# Add NewsAPI key to Key Vault
az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "newsapi-key" \
  --value "e77a08f48bcd4d2a8feb1a8458888042"
  
az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-user" \
  --value "dataexpert_student"
  
  
  az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-password" \
  --value "DataExpert123!"
  
  
  az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-database" \
  --value "dataexpert_student"
  
  
  az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-account" \
  --value "aab46027"
  
  az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-role" \
  --value "all_users_role"
  
  az keyvault secret set \
  --vault-name kv-stock-sentiment-2025 \
  --name "sf-warehouse" \
  --value "COMPUTE_WH"  