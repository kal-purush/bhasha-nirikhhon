using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.AzureKeyVault;
using System;
using System.Collections.Generic;
using System.Text;

namespace Photos.Application.Extensions
{
    public static class ConfigurationBuilderExtensions
    {
        public static void AddAzureKeyVault(this IConfigurationBuilder configurationBuilder)
        {
            var configuration = configurationBuilder.Build();

            var keyVault = configuration.GetSection("AzureKeyVault");
          
            var azureServiceTokenProvider = new AzureServiceTokenProvider();
            var keyVaultClient = new KeyVaultClient(
                new KeyVaultClient.AuthenticationCallback(
                    azureServiceTokenProvider.KeyVaultTokenCallback));

            configurationBuilder.AddAzureKeyVault($"https://{keyVault.Value}.vault.azure.net/",
                 keyVaultClient, new DefaultKeyVaultSecretManager());        
        }
    }
}