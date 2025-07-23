#!/bin/bash

# =============================================================================
# AZURE RESOURCE CREATION SCRIPT - TEMPLATE VERSION
# =============================================================================
# 
# ⚠️  SECURITY WARNING: This script contains placeholder values for secrets.
#     Replace the placeholder values with your actual secrets before running.
#     NEVER commit actual secrets to version control!
#
# =============================================================================
# SETUP INSTRUCTIONS:
# =============================================================================
# 1. Replace all placeholder values (marked with <YOUR_ACTUAL_VALUE>) with real secrets
# 2. Run this script to create Azure resources
# 3. Store secrets securely in Azure Key Vault
# 4. Use environment variables or secure methods to access secrets in your application
# =============================================================================

# Set variables
RESOURCE_GROUP="rg-stock-sentiment-analysis"
LOCATION="eastus2"
STORAGE_ACCOUNT="dlsstocksentiment2025"
EVENT_HUB_NAMESPACE="eh-stock-sentiment-2025"
EVENT_HUB_NAME="stock-data-hub"
KEY_VAULT_NAME="kv-stock-sentiment-2025"
DATABRICKS_WORKSPACE="databricks-stock-sentiment-canada"

echo "🚀 Creating Azure resources for Stock Sentiment Analysis project..."
echo "📍 Location: $LOCATION"
echo "📦 Resource Group: $RESOURCE_GROUP"

# Create resource group
echo "📋 Creating resource group..."
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# Create storage account
echo "💾 Creating storage account..."
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --https-only true \
  --min-tls-version TLS1_2

# Get storage account key (this will be used to create connection string)
STORAGE_KEY=$(az storage account keys list \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query '[0].value' \
  --output tsv)

# Create Event Hub namespace
echo "📡 Creating Event Hub namespace..."
az eventhubs namespace create \
  --name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard \
  --enable-auto-inflate false \
  --maximum-throughput-units 0

# Create Event Hub
echo "📡 Creating Event Hub..."
az eventhubs eventhub create \
  --name $EVENT_HUB_NAME \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --message-retention 1 \
  --partition-count 2

# Get Event Hub connection string
EVENT_HUB_CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString \
  --output tsv)

# Create Key Vault
echo "🔐 Creating Key Vault..."
az keyvault create \
  --name $KEY_VAULT_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku standard

# Create Databricks workspace
echo "🖥️ Creating Databricks workspace..."
az databricks workspace create \
  --name $DATABRICKS_WORKSPACE \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku standard

# =============================================================================
# STORE SECRETS IN KEY VAULT (REPLACE PLACEHOLDER VALUES!)
# =============================================================================

echo "🔐 Storing secrets in Key Vault..."
echo "⚠️  IMPORTANT: Replace placeholder values with your actual secrets!"

# Add Event Hub connection string (REPLACE WITH YOUR ACTUAL CONNECTION STRING)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "event-hub-connection-string" \
  --value "REPLACE_WITH_YOUR_ACTUAL_EVENT_HUB_CONNECTION_STRING"

# Add Storage connection string (REPLACE WITH YOUR ACTUAL CONNECTION STRING)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "storage-connection-string" \
  --value "REPLACE_WITH_YOUR_ACTUAL_STORAGE_CONNECTION_STRING"

# Add Polygon API key (REPLACE WITH YOUR ACTUAL API KEY)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "polygon-api-key" \
  --value "REPLACE_WITH_YOUR_ACTUAL_POLYGON_API_KEY"

# Add NewsAPI key (REPLACE WITH YOUR ACTUAL API KEY)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "newsapi-key" \
  --value "REPLACE_WITH_YOUR_ACTUAL_NEWSAPI_KEY"

# Add Snowflake credentials (REPLACE WITH YOUR ACTUAL CREDENTIALS)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "sf-user" \
  --value "REPLACE_WITH_YOUR_ACTUAL_SNOWFLAKE_USERNAME"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "sf-password" \
  --value "REPLACE_WITH_YOUR_ACTUAL_SNOWFLAKE_PASSWORD"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "sf-database" \
  --value "REPLACE_WITH_YOUR_ACTUAL_SNOWFLAKE_DATABASE"

# =============================================================================
# SETUP DATABRICKS ACCESS TO KEY VAULT
# =============================================================================

echo "🔗 Setting up Databricks access to Key Vault..."

# Get your subscription ID
SUBSCRIPTION_ID=$(az account show --query id --output tsv)

# Create service principal with Key Vault access
echo "👤 Creating service principal for Databricks..."
az ad sp create-for-rbac \
  --name "sp-databricks-keyvault" \
  --role "Key Vault Secrets User" \
  --scopes "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KEY_VAULT_NAME"

# Get Databricks workspace details
echo "📊 Getting Databricks workspace details..."
az databricks workspace show \
  --name $DATABRICKS_WORKSPACE \
  --resource-group $RESOURCE_GROUP \
  --query "{name:name, id:id, managedResourceGroupId:managedResourceGroupId}" \
  --output table

# =============================================================================
# OUTPUT SUMMARY
# =============================================================================

echo ""
echo "✅ Azure resources created successfully!"
echo ""
echo "📋 RESOURCE SUMMARY:"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Storage Account: $STORAGE_ACCOUNT"
echo "   Event Hub Namespace: $EVENT_HUB_NAMESPACE"
echo "   Event Hub: $EVENT_HUB_NAME"
echo "   Key Vault: $KEY_VAULT_NAME"
echo "   Databricks Workspace: $DATABRICKS_WORKSPACE"
echo ""
echo "⚠️  NEXT STEPS:"
echo "   1. Replace placeholder values in this script with your actual secrets"
echo "   2. Re-run the secret storage commands with real values"
echo "   3. Configure Databricks to access Key Vault"
echo "   4. Test your pipeline with the new resources"
echo ""
echo "🔐 SECURITY REMINDER:"
echo "   - Never commit actual secrets to version control"
echo "   - Use environment variables or secure methods to access secrets"
echo "   - Regularly rotate your API keys and access tokens"
echo ""  