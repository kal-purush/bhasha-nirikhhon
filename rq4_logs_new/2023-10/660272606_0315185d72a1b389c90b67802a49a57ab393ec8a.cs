using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using System;
using Azure.Storage.Queues;
using Azure.Identity;
using Azure.Data.Tables;
using EST.MIT.InvoiceImporter.Function.Interfaces;
using EST.MIT.InvoiceImporter.Function.DataAccess;
using Azure.Storage.Blobs;
using AutoMapper;

namespace EST.MIT.InvoiceImporter.Function.Services
{
    /// <summary>
    /// Register service-tier services.
    /// </summary>
    public static class ServicesConfiguration
    {
        /// <summary>
        /// Method to register service-tier services.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        /// <param name="mapperConfig">Automapper config</param>
        public static void AddTableBlobQueueServices(this IServiceCollection services, IConfiguration configuration, MapperConfiguration mapperConfig)
        {
        services.AddSingleton<IAzureTableService>(_ =>
        {
            var tableStorageAccountCredential = configuration.GetSection("TableConnectionString:Credential").Value;
            if (IsManagedIdentity(tableStorageAccountCredential))
            {
                var tableServiceUri = new Uri(configuration.GetSection("TableConnectionString:TableServiceUri").Value);
                Console.WriteLine($"Startup.TableClient using Managed Identity with url {tableServiceUri}");
                return new AzureTableService(new TableClient(tableServiceUri, "importrequests", new DefaultAzureCredential()), mapperConfig.CreateMapper());
            }
            else
            {
                return new AzureTableService(new TableClient(configuration.GetSection("TableConnectionString").Value, "importrequests"), mapperConfig.CreateMapper());
            }
        });

        services.AddSingleton<IAzureBlobService>(_ =>
        {
            var blobStorageAccountCredential = configuration.GetSection("BlobConnectionString:Credential").Value;
            if (IsManagedIdentity(blobStorageAccountCredential))
            {
                var blobServiceUri = new Uri(configuration.GetSection("BlobConnectionString:BlobServiceUri").Value);
                Console.WriteLine($"Startup.BlobClient using Managed Identity with url {blobServiceUri}");
                return new AzureBlobService(new BlobServiceClient(blobServiceUri, new DefaultAzureCredential()));
            }
            else
            {
                return new AzureBlobService(new BlobServiceClient(configuration.GetSection("BlobConnectionString").Value));
            }
        });


        services.AddSingleton<IEventQueueService>(_ =>
        {
            var eventQueueName = configuration.GetSection("EventQueueName").Value;
            var queueConnectionString = configuration.GetSection("QueueConnectionString:Credential").Value;
            var managedIdentityNamespace = configuration.GetSection("QueueConnectionString:fullyQualifiedNamespace").Value;

            if (IsManagedIdentity(queueConnectionString))
            {
                var queueServiceUri = configuration.GetSection("QueueConnectionString:QueueServiceUri").Value;
                var queueUrl = new Uri($"{queueServiceUri}{eventQueueName}");
                return new EventQueueService(new QueueClient(queueUrl, new DefaultAzureCredential()));
            }
            else
            {
                return new EventQueueService(new QueueClient(configuration.GetSection("QueueConnectionString").Value, eventQueueName));
            }
        });
       }

        private static bool IsManagedIdentity(string credentialName)
        {
            return credentialName != null && credentialName.ToLower() == "managedidentity";
        }
    }
}