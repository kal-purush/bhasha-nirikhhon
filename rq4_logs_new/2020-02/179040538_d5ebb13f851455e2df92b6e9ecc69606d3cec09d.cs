using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Dfc.ProviderPortal.Packages.AzureFunctions.DependencyInjection;
using Dfc.ProviderPortal.TribalExporter.Interfaces;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Dfc.ProviderPortal.TribalExporter.Functions
{
    public static class ArchiveCourses
    {
        [FunctionName(nameof(ArchiveCourses))]
        [NoAutomaticTrigger]
        public static async Task Run(
            string input,  // Work around https://github.com/Azure/azure-functions-vs-build-sdk/issues/168
            [Inject] IConfigurationRoot configuration,
            [Inject] ICosmosDbHelper cosmosDbHelper,
            [Inject] IBlobStorageHelper blobHelper,
            [Inject] ILoggerFactory loggerFactory)
        {
            var whitelistFileName = "ProviderWhiteList.txt";
            var blobContainer = configuration["BlobStorageSettings:Container"];
            var databaseId = configuration["CosmosDbSettings:DatabaseId"];
            var coursesCollectionId = "courses";

            var logger = loggerFactory.CreateLogger(typeof(ArchiveCourses));

            var sprocLink = UriFactory.CreateStoredProcedureUri(databaseId, coursesCollectionId, "ArchiveCoursesForProvider");

            var whitelist = await GetProviderWhiteList();

            using (var documentClient = cosmosDbHelper.GetClient())
            {
                foreach (var ukprn in whitelist)
                {
                    var response = await documentClient.ExecuteStoredProcedureAsync<int>(
                        sprocLink,
                        new RequestOptions()
                        {
                            PartitionKey = new Microsoft.Azure.Documents.PartitionKey(ukprn)
                        },
                        ukprn);
                    var updated = response.Response;
                    logger.LogInformation($"Archived {updated} courses for {ukprn}");
                }
            }

            async Task<ISet<int>> GetProviderWhiteList()
            {
                var blob = blobHelper.GetBlobContainer(blobContainer).GetBlockBlobReference(whitelistFileName);

                var ms = new MemoryStream();
                await blob.DownloadToStreamAsync(ms);
                ms.Seek(0L, SeekOrigin.Begin);

                var results = new HashSet<int>();
                string line;
                using (var reader = new StreamReader(ms))
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (string.IsNullOrEmpty(line))
                        {
                            continue;
                        }

                        var ukprn = int.Parse(line);
                        results.Add(ukprn);
                    }
                }

                return results;
            }
        }
    }
}