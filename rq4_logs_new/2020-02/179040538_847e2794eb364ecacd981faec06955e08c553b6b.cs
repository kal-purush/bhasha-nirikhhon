using CsvHelper;
using Dfc.CourseDirectory.Models.Models.Providers;
using Dfc.ProviderPortal.Packages.AzureFunctions.DependencyInjection;
using Dfc.ProviderPortal.TribalExporter.Interfaces;
using Dfc.ProviderPortal.TribalExporter.Models.Tribal;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Dfc.ProviderPortal.TribalExporter.Functions
{
    public static class ProviderMigrator
    {
        [FunctionName(nameof(ProviderMigrator))]
        [NoAutomaticTrigger]
        public static async Task Run(
                    string input,  // Work around https://github.com/Azure/azure-functions-vs-build-sdk/issues/168
                    ILogger log,
                    [Inject] IConfigurationRoot configuration,
                    [Inject] IProviderCollectionService providerCollectionService,
                    [Inject] ICosmosDbHelper cosmosDbHelper,
                    [Inject] IBlobStorageHelper blobhelper,
                    [Inject] IUkrlpApiService ukrlpApiService
                    )
        {
            const string WHITE_LIST_FILE = "ProviderWhiteList.txt";

            var stopWatch = new Stopwatch();

            // TODO : Change to correct collection below
            var databaseId = configuration["CosmosDbSettings:DatabaseId"];
            var providerCollectionId = configuration["CosmosDbCollectionSettings:ProvidersCollectionId"];
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var venueExportFileName = $"ProviderExport-{DateTime.Now.ToString("dd-MM-yy HHmm")}";
            
            var blobContainer = blobhelper.GetBlobContainer(configuration["BlobStorageSettings:Container"]);

            log.LogInformation($"WhitelistProviders : Start reading...");
            stopWatch.Start();
            var whiteListProviders = await GetProviderWhiteList();
            stopWatch.Start();
            log.LogInformation($"WhitelistProviders : Finished reading in {stopWatch.ElapsedMilliseconds / 1000} seconds.");


            // Get all changed data from UKRLP API
            stopWatch.Reset();
            log.LogInformation($"UKRLApiService: Start getting data..");
            stopWatch.Start();
            var ukrlpApiProviders = ukrlpApiService.GetAllProviders();
            stopWatch.Stop();
            log.LogInformation($"UKRLApiService: Finished getting datain {stopWatch.ElapsedMilliseconds / 1000}.");

            var result = new List<ProviderResultMessage>();
            var ukprnCache = new List<int>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                using (var command = sqlConnection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = @"SELECT 
                                                    P.ProviderId,
                                                    P.Ukprn,
                                                    P.ProviderName,
                                                    RS.RecordStatusId,
                                                    RS.RecordStatusName,
		                                            P.RoATPFFlag,
		                                            P.RoATPProviderTypeId,
		                                            P.RoATPStartDate,
		                                            p.PassedOverallQAChecks
                                            FROM [Tribal].[Provider] P
                                            JOIN [Tribal].[RecordStatus] RS
                                            ON P.RecordStatusId = RS.RecordStatusId
                                            WHERE P.RecordStatusId = 2
                                            ";

                    try
                    {
                        //Open connection.
                        sqlConnection.Open();

                        log.LogInformation($"Tribal Data: Start....");

                        using (SqlDataReader dataReader = command.ExecuteReader())
                        {
                            while (dataReader.Read())
                            {
                                // 1) Read provider data from Tribal
                                var item = ProviderSource.FromDataReader(dataReader);

                                stopWatch.Reset();
                                log.LogInformation($"Processing Provider: {item.ProviderId} with Ukprn {item.UKPRN}.");

                                stopWatch.Start();
                                // 2) Check if in Whitelist
                                if (!whiteListProviders.Any(x => x == item.UKPRN))
                                {
                                    AddResultMessage(item.ProviderId, "SKIPPED-NotOnWhitelist", $"Provider {item.ProviderId} not on whitelist, ukprn {item.UKPRN}");
                                    continue;
                                }

                                // 3) Check againts API ? If no match Add to Result Message, skip next
                                var ukrlpProviderItem = ukrlpApiProviders.FirstOrDefault(p => p.UnitedKingdomProviderReferenceNumber.Trim() == item.UKPRN.ToString());
                                if (ukrlpProviderItem == null)
                                {
                                    AddResultMessage(item.ProviderId, "SKIPPED-NotInUkrlpApi", $"Provider {item.ProviderId} cannot be found in UKRLP Api, ukprn {item.UKPRN}");
                                    continue;
                                }

                                // 4) Build Cosmos collection record
                                var cosmosProviderItem = await providerCollectionService.GetDocumentByUkprn(item.UKPRN);
                                
                                if(cosmosProviderItem != null)
                                {
                                    AddResultMessage(item.ProviderId, "PROCESSED-Updated", $"Provider {item.ProviderId} updated in Cosmos Collection, ukprn {item.UKPRN}");
                                }
                                else
                                {
                                    AddResultMessage(item.ProviderId, "PROCESSED-Inserted", $"Provider {item.ProviderId} inserted in Cosmos Collection, ukprn {item.UKPRN}");
                                }

                                stopWatch.Stop();

                                log.LogInformation($"Processed Provider {item.ProviderId} with Ukprn {item.UKPRN} in {stopWatch.ElapsedMilliseconds / 1000} seconds.");
                            }
                            dataReader.Close();
                        }

                        log.LogInformation($"Tribal Data: Processing completed.");
                    }
                    catch(Exception ex)
                    {
                        log.LogError(ex.Message);
                    }

                    var resultsObjBytes = GetResultAsByteArray(result);
                    await WriteResultsToBlobStorage(resultsObjBytes);
                }
            }

            void AddResultMessage(int providerId, string status, string message = "")
            {
                var validateResult = new ProviderResultMessage() { ProviderId = providerId, Status = status, Message = message };
                result.Add(validateResult);
            }

            async Task<IList<int>> GetProviderWhiteList()
            {
                var list = new List<int>();
                var whiteList = await blobhelper.ReadFileAsync(blobContainer, WHITE_LIST_FILE);
                if (!string.IsNullOrEmpty(whiteList))
                {
                    var lines = whiteList.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                    foreach (string line in lines)
                    {
                        if (int.TryParse(line, out int id))
                        {
                            list.Add(id);
                        }
                    }
                }
                return list;
            }

            byte[] GetResultAsByteArray(IList<ProviderResultMessage> message)
            {
                using (var memoryStream = new System.IO.MemoryStream())
                {
                    using (var streamWriter = new System.IO.StreamWriter(memoryStream))
                    using (var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture))
                    {
                        csvWriter.WriteRecords<ProviderResultMessage>(message);
                    }
                    return memoryStream.ToArray();
                }
            }

            async Task WriteResultsToBlobStorage(byte[] data)
            {
                await blobhelper.UploadFile(blobContainer, venueExportFileName, data);
            }
        }
    }
}

[Serializable()]
public class ProviderResultMessage
{
    public int ProviderId { get; set; }
    public string Status { get; set; }
    public string Message { get; set; }
}