# 🔐 Security Guide - Stock Sentiment Analysis Project

## ⚠️ IMPORTANT SECURITY NOTICE

This project contains sensitive information and API keys. Follow these security best practices to keep your data safe.

## 🚨 What Was Fixed

The original `scripts/create_az_resources.sh` file contained hardcoded secrets that GitHub's push protection detected:
- Azure Event Hub connection strings
- Azure Storage Account access keys
- Polygon API keys
- NewsAPI keys
- Snowflake credentials

**These have been replaced with placeholder values for security.**

## 🔧 How to Use the Script Safely

### 1. Replace Placeholder Values

In `scripts/create_az_resources.sh`, replace all placeholder values:

```bash
# Replace these with your actual values:
<YOUR_ACTUAL_POLYGON_API_KEY>
<YOUR_ACTUAL_NEWSAPI_KEY>
<YOUR_ACTUAL_SHARED_ACCESS_KEY>
<YOUR_ACTUAL_STORAGE_KEY>
<YOUR_ACTUAL_SNOWFLAKE_USERNAME>
<YOUR_ACTUAL_SNOWFLAKE_PASSWORD>
```

### 2. Use Environment Variables

Instead of hardcoding secrets, use environment variables:

```bash
# Set environment variables
export POLYGON_API_KEY="your_actual_key"
export NEWSAPI_KEY="your_actual_key"
export AZURE_STORAGE_KEY="your_actual_key"

# Use in script
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "polygon-api-key" \
  --value "$POLYGON_API_KEY"
```

### 3. Use Azure Key Vault (Recommended)

Store all secrets in Azure Key Vault and access them securely:

```python
# In your Databricks notebooks
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-keyvault.vault.azure.net/", credential=credential)

# Retrieve secrets
polygon_key = client.get_secret("polygon-api-key").value
newsapi_key = client.get_secret("newsapi-key").value
```

## 📋 Security Checklist

- [ ] ✅ No hardcoded secrets in version control
- [ ] ✅ All API keys stored in Azure Key Vault
- [ ] ✅ Environment variables used for local development
- [ ] ✅ Secrets rotated regularly
- [ ] ✅ Access logs monitored
- [ ] ✅ Principle of least privilege applied

## 🛡️ Best Practices

### 1. Never Commit Secrets
```bash
# ❌ DON'T DO THIS
echo "api_key=secret123" >> config.txt
git add config.txt

# ✅ DO THIS INSTEAD
echo "api_key=<PLACEHOLDER>" >> config.txt
git add config.txt
```

### 2. Use .env Files (Local Development)
```bash
# Create .env file (not tracked by git)
echo "POLYGON_API_KEY=your_actual_key" > .env
echo "NEWSAPI_KEY=your_actual_key" >> .env

# Load in your application
source .env
```

### 3. Use Azure Key Vault (Production)
```python
# Secure way to access secrets
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret(secret_name):
    credential = DefaultAzureCredential()
    client = SecretClient(
        vault_url=os.environ["KEY_VAULT_URL"], 
        credential=credential
    )
    return client.get_secret(secret_name).value
```

### 4. Rotate Secrets Regularly
- API keys: Every 90 days
- Database passwords: Every 60 days
- Access tokens: Every 30 days

## 🔍 Monitoring and Alerts

Set up monitoring for:
- Failed authentication attempts
- Unusual API usage patterns
- Access to sensitive resources
- Secret access logs

## 📞 Emergency Procedures

If secrets are compromised:

1. **Immediate Actions:**
   - Revoke compromised keys immediately
   - Rotate all related secrets
   - Check access logs for unauthorized use

2. **Investigation:**
   - Review git history for exposed secrets
   - Check for unauthorized access
   - Update security policies

3. **Recovery:**
   - Generate new secrets
   - Update all applications
   - Test functionality

## 📚 Additional Resources

- [Azure Key Vault Documentation](https://docs.microsoft.com/en-us/azure/key-vault/)
- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [OWASP Security Guidelines](https://owasp.org/www-project-top-ten/)
- [Azure Security Best Practices](https://docs.microsoft.com/en-us/azure/security/)

## 🆘 Getting Help

If you need assistance with security:
1. Check the Azure documentation
2. Review GitHub's security features
3. Contact your security team
4. Use Azure Security Center for monitoring

---

**Remember: Security is everyone's responsibility!** 🔒 