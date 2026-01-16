using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dfc.ProviderPortal.Packages.AzureFunctions.DependencyInjection;
using Dfc.ProviderPortal.TribalExporter.Interfaces;
using Dfc.ProviderPortal.TribalExporter.Models.Tribal;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;
using System.Text;
using CsvHelper;
using System.Globalization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Azure.Documents.Client;
using Dfc.CourseDirectory.Models.Enums;
using Dfc.CourseDirectory.Services.Interfaces;
using Dapper;
using Dfc.CourseDirectory.Services;
using Dfc.CourseDirectory.Services.ApprenticeshipService;
using Dfc.CourseDirectory.Models.Models.ApprenticeshipReferenceData;
using Dfc.CourseDirectory.Services.Interfaces.ApprenticeshipService;
using Dfc.CourseDirectory.Models.Models.Providers;
using Dfc.CourseDirectory.Models.Models.Apprenticeships;

namespace Dfc.ProviderPortal.TribalExporter.Functions
{
    public static class ApprenticeshipMigrator
    {
        [FunctionName(nameof(ApprenticeshipMigrator))]
        [NoAutomaticTrigger]
        public static async Task Run(
                    string input,  // Work around https://github.com/Azure/azure-functions-vs-build-sdk/issues/168
                    ILogger log,
                    [Inject] IConfigurationRoot configuration,
                    [Inject] IProviderCollectionService providerCollectionService,
                    [Inject] ICosmosDbHelper cosmosDbHelper,
                    [Inject] IBlobStorageHelper blobhelper,
                    [Inject] IApprenticeReferenceDataService apprenticeReferenceDataService,
                    [Inject] IApprenticeshipServiceWrapper apprenticeshipService
                    )
        {
            var apprenticeshipCollectionId = configuration["CosmosDbCollectionSettings:ApprenticeshipCollectionId"];
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            var blobContainer = blobhelper.GetBlobContainer(configuration["BlobStorageSettings:Container"]);
            var whiteListProviders = await GetProviderWhiteList();
            var result = new List<ResultMessage>();
            var venueExportFileName = $"ApprenticeshipExport-{DateTime.Now.ToString("dd-MM-yy HHmm")}";
            const string WHITE_LIST_FILE = "ProviderWhiteList.txt";
            var ukprnCache = new List<int>();
            var databaseId = configuration["CosmosDbSettings:DatabaseId"];

            var apprenticeshipSQL = @"SELECT a.ApprenticeshipId,
	                                           p.ProviderId,
	                                           a.FrameworkCode,
	                                           a.ProgType,
	                                           a.PathwayCode,
	                                           a.StandardCode,
	                                           a.[Version],
	                                           a.MarketingInformation,
	                                           a.[Url],
	                                           a.ContactEmail,
	                                           a.ContactTelephone,
	                                           a.ContactWebsite,
	                                           a.RecordStatusId,
	                                           a.CreatedByUserId,
	                                           a.CreatedDateTimeUtc,
	                                           p.Ukprn
                                        FROM tribal.Apprenticeship a
                                        INNER JOIN tribal.Provider p on p.ProviderId = a.ProviderId
                                        INNER JOIN tribal.ApprenticeshipQACompliance apc on apc.ApprenticeshipId = a.ApprenticeshipId
                                        WHERE a.RecordStatusId = 2
                                        ORDER BY ProviderId
                                        ";
            var apprenticeshipLocationsSQL = @"SELECT al.ApprenticeshipId,
	                                           al.ApprenticeshipLocationId,
	                                           l.LocationId,
	                                           a.AddressId,
	                                           a.AddressLine1,
	                                           a.AddressLine2,
	                                           a.County,
	                                           a.Postcode,
                                               a.Town,
	                                           a.Longitude,
	                                           a.Latitude,
	                                           l.Website,
	                                           l.Email,
	                                           als.CSV as DeliveryModeStr,
                                               l.Telephone,
	                                           l.LocationName,
	                                           p.ProviderId,
	                                           p.Ukprn,
                                               al.Radius
                                        FROM Tribal.ApprenticeshipLocation al
                                        INNER JOIN Tribal.Location l on l.LocationId = al.LocationId
                                        INNER JOIN Tribal.Provider p on p.ProviderId = l.ProviderId
                                        INNER JOIN Tribal.Address a ON a.AddressId = l.AddressId
                                        CROSS APPLY (SELECT STRING_AGG(DeliveryModeId,',') as CSV, 
					                                        aldm.ApprenticeshipLocationId
			                                         FROM tribal.ApprenticeshipLocationDeliveryMode aldm
		                                             WHERE aldm.ApprenticeshipLocationId = al.ApprenticeshipLocationId
			                                         GROUP BY aldm.ApprenticeshipLocationId
			                                         ) als
                                        WHERE al.RecordStatusId = 2 and al.ApprenticeshipId = @ApprenticeshipId
                                        ORDER BY ApprenticeshipId,ApprenticeshipLocationId";


            try
            {
                using (var conn1 = new SqlConnection(connectionString))
                {
                    await conn1.OpenAsync();

                    using (var apprenticeshipscmd = conn1.CreateCommand())
                    {
                        apprenticeshipscmd.CommandText = apprenticeshipSQL;

                        using (var apprenticeshipReader = apprenticeshipscmd.ExecuteReader())
                        {
                            while (await apprenticeshipReader.ReadAsync())
                            {
                                var item = ApprenticeshipResult.FromDataReader(apprenticeshipReader);
                                var (validated, exisitingApprenticeship, referenceDataFramework, ReferenceDataStandard, locations, provider) = await Validate(item);
                                if (validated)
                                {
                                    await CreateOrUpdateApprenticeshipRecord(item, exisitingApprenticeship, locations, referenceDataFramework, ReferenceDataStandard, provider);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError("Error occured Migrating Apprenticeships", e.Message);
            }

            //Log Results to blob storage
            var resultsObjBytes = GetResultAsByteArray(result);
            await WriteResultsToBlobStorage(resultsObjBytes);

            //log completion
            log.LogInformation("Migrating Apprenticeships Complete");

            async Task CreateOrUpdateApprenticeshipRecord(ApprenticeshipResult tribalRecord, Apprenticeship existingApprenticeship,
                IList<ApprenticeshipLocationResult> locations, ReferenceDataFramework refDataFramework, ReferenceDateStandard refDataStandard,
                Provider provider)
            {
                var apprenticeshipTitle = tribalRecord.FrameworkCode.HasValue ? refDataFramework?.NasTitle : refDataStandard?.StandardName;
                var nvqLevel2 = refDataStandard?.NotionalEndLevel;

                var id = existingApprenticeship?.id.ToString() ?? Guid.NewGuid().ToString();

                var s = UriFactory.CreateDocumentUri(databaseId, apprenticeshipCollectionId, id);
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, apprenticeshipCollectionId);
                var cosmosApprenticeship = new ApprenticeshipDTO()
                {
                    id = id,
                    ApprenticeshipID = tribalRecord.ApprenticeshipID,
                    ApprenticeshipTitle = apprenticeshipTitle,
                    ProviderId = provider.id.ToString(),
                    PathWayCode = tribalRecord.PathWayCode,
                    ProgType = tribalRecord.ProgType,
                    ProviderUKPRN = tribalRecord.UKPRN,
                    FrameworkId = refDataFramework?.Id.ToString(),
                    StandardId = refDataStandard?.id.ToString(),
                    FrameworkCode = tribalRecord.FrameworkCode,
                    StandardCode = tribalRecord.StandardCode,
                    Version = tribalRecord.Version,
                    MarketingInformation = tribalRecord.MarketingInformation,
                    Url = tribalRecord.Url,
                    ContactTelephone = tribalRecord.ContactTelephone,
                    ContactEmail = tribalRecord.ContactEmail,
                    ContactWebsite = tribalRecord.ContactWebsite,
                    CreatedBy = "ApprenticeshipMigrator",
                    CreatedDate = DateTime.Now,
                    NotionalNVQLevelv2 = nvqLevel2,
                    ApprenticeshipLocations = MapLocations(locations),
                    ApprenticeshipType = MapApprenticeshipType(tribalRecord)
                };

                await cosmosDbHelper.GetClient().UpsertDocumentAsync(collectionUri, cosmosApprenticeship);
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

            async Task WriteResultsToBlobStorage(byte[] data)
            {
                await blobhelper.UploadFile(blobContainer, venueExportFileName, data);
            }

            void AddResultMessage(int apprenticeshipId, string status, string message = "")
            {
                var validateResult = new ResultMessage() { ApprenticeshipID = apprenticeshipId, Status = status, Message = message };
                result.Add(validateResult);
            }

            byte[] GetResultAsByteArray(IList<ResultMessage> ob)
            {
                using (var memoryStream = new System.IO.MemoryStream())
                {
                    using (var streamWriter = new System.IO.StreamWriter(memoryStream))
                    using (var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture))
                    {
                        csvWriter.WriteRecords<ResultMessage>(ob);
                    }
                    return memoryStream.ToArray();
                }
            }

            async Task<(bool, Apprenticeship, ReferenceDataFramework, ReferenceDateStandard, IList<ApprenticeshipLocationResult>, Provider)> Validate(ApprenticeshipResult item)
            {
                var referenceDataFramework = default(ReferenceDataFramework);
                var referenceDataStandard = default(ReferenceDateStandard);
                var locations = new List<ApprenticeshipLocationResult>();
                var provider = default(Provider);
                var existingApprenticeship = default(Apprenticeship);

                //are providers on list of whitelisted providers file
                if (!whiteListProviders.Any(x => x == item.UKPRN))
                {
                    AddResultMessage(item.ApprenticeshipID, "Failed", $"Provider {item.ProviderId} not on whitelist, ukprn {item.UKPRN}");
                    return (false, null, null, null, null, null);
                }

                //check to see if a record is already held for ukprn
                if (!ukprnCache.Contains(item.UKPRN))
                {
                    provider = await providerCollectionService.GetDocumentByUkprn(item.UKPRN);
                    if (provider == null)
                    {
                        AddResultMessage(item.ApprenticeshipID, "Failed", "Unknown UKPRN");
                        return (false, null, null, null, null, null);
                    }
                    else
                    {
                        //provider exists - add to cache
                        ukprnCache.Add(item.UKPRN);
                    }
                }

                //check apprenticeship framework/standard returns a record
                if (item.FrameworkCode.HasValue)
                {
                    var apprenticeship = await apprenticeReferenceDataService.GetFrameworkByCode(item.FrameworkCode ?? 0, item.ProgType ?? 0, item.PathWayCode ?? 0);
                    referenceDataFramework = apprenticeship.Value.Value;

                    //unknown framework
                    if (!apprenticeship.HasValue)
                    {
                        AddResultMessage(item.ApprenticeshipID, "Failed", $"Framework {item.FrameworkCode}, ProgType: {item.ProgType}, pathwayCode: {item.PathWayCode} does not exist");
                        return (false, null, null, null, null, null);
                    }
                }
                else
                {
                    var apprenticeship = await apprenticeReferenceDataService.GetStandardById(item.StandardCode ?? 0, item.Version ?? 0);
                    referenceDataStandard = apprenticeship.Value.Value;

                    //unknown standard
                    if (!apprenticeship.HasValue)
                    {
                        AddResultMessage(item.ApprenticeshipID, "Failed", $"Standard {item.StandardCode}, version: {item.Version} does not exist");
                        return (false, null, null, null, null, null);
                    }
                }

                //get locations for apprenticeship
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    locations = sqlConnection.Query<ApprenticeshipLocationResult>(apprenticeshipLocationsSQL, new { apprenticeshipId = item.ApprenticeshipID }, commandType: CommandType.Text).ToList();
                }

                //fetch existing apprenticeship row.
                existingApprenticeship = await apprenticeshipService.GetApprenticeshipByApprenticeshipID(item.ApprenticeshipID);

                return (true, existingApprenticeship, referenceDataFramework, referenceDataStandard, locations, provider);
            }
        }

        private static int MapApprenticeshipType(ApprenticeshipResult tribalRecord)
        {
            if (tribalRecord.StandardCode.HasValue)
                return (int)ApprenticeshipType.StandardCode;
            else if (tribalRecord.FrameworkCode.HasValue)
                return (int)ApprenticeshipType.FrameworkCode;
            else
                return (int)ApprenticeshipType.Undefined;
        }

        private static IList<ApprenticeshipLocationDTO> MapLocations(IList<ApprenticeshipLocationResult> locations)
        {
            var lst = new List<ApprenticeshipLocationDTO>();

            foreach (var apprenticeshipLocation in locations)
            {
                var appLocation = new ApprenticeshipLocationDTO()
                {
                    Id = Guid.NewGuid().ToString(),
                    VenueId = Guid.Empty.ToString(),
                    Address = new Dfc.CourseDirectory.Models.Models.Apprenticeships.Address()
                    {
                        Address1 = apprenticeshipLocation.AddressLine1,
                        Address2 = apprenticeshipLocation.AddressLine2,
                        County = apprenticeshipLocation.County,
                        Email = apprenticeshipLocation.Email,
                        Website = apprenticeshipLocation.Website,
                        Longitude = apprenticeshipLocation.Longitude,
                        Latitude = apprenticeshipLocation.Latitude,
                        Postcode = apprenticeshipLocation.Postcode,
                        Town = apprenticeshipLocation.Town,
                        Phone = apprenticeshipLocation.Telephone
                    },
                    DeliveryModes = apprenticeshipLocation.DeliveryModes,
                    LocationId = apprenticeshipLocation.LocationId,
                    Name = apprenticeshipLocation.LocationName,
                    ProviderId = apprenticeshipLocation.ProviderId,
                    ProviderUKPRN = apprenticeshipLocation.UKPRN,
                    Radius = apprenticeshipLocation.Radius
                };

                lst.Add(appLocation);
            }

            return lst;
        }
    }

    public class ApprenticeshipResult
    {
        public int ApprenticeshipID { get; set; }
        public int? FrameworkCode { get; set; }
        public int UKPRN { get; set; }
        public int? StandardCode { get; set; }
        public int? PathWayCode { get; set; }
        public int? ProgType { get; set; }
        public int? Version { get; set; }
        public int ProviderId { get; set; }
        public string MarketingInformation { get; set; }
        public string Url { get; set; }
        public string ContactEmail { get; set; }
        public string ContactTelephone { get; set; }
        public string ContactWebsite { get; set; }
        public int RecordStatusId { get; set; }
        public string CreatedByUserId { get; set; }
        public DateTime CreatedDateTimeUtc { get; set; }


        public static ApprenticeshipResult FromDataReader(SqlDataReader reader)
        {
            var result = new ApprenticeshipResult()
            {
                ApprenticeshipID = (int)reader["ApprenticeshipID"],
                FrameworkCode = reader["Frameworkcode"] as int?,
                UKPRN = (int)reader["UKPRN"],
                PathWayCode = reader["PathWayCode"] as int?,
                ProgType = reader["ProgType"] as int?,
                Version = reader["Version"] as int?,
                StandardCode = reader["StandardCode"] as int?,
                ProviderId = (int)reader["ProviderId"],
                MarketingInformation = reader["MarketingInformation"] as string,
                Url = reader["URL"] as string,
                ContactEmail = reader["ContactEmail"] as string,
                ContactTelephone = reader["ContactTelephone"] as string,
                ContactWebsite = reader["ContactWebsite"] as string,
                RecordStatusId = (int)reader["RecordStatusId"],
                CreatedByUserId = reader["CreatedByUserId"] as string,
                CreatedDateTimeUtc = (DateTime)reader["CreatedDateTimeUtc"]
            };
            return result;
        }
    }

    public class ApprenticeshipLocationResult
    {
        public int LocationId { get; set; }
        public int AddressId { get; set; }
        public string AddressLine1 { get; set; }
        public string AddressLine2 { get; set; }
        public string County { get; set; }
        public string Postcode { get; set; }
        public string Town { get; set; }
        public string Telephone { get; set; }
        public string LocationName { get; set; }
        public long? Longitude { get; set; }
        public long? Latitude { get; set; }
        public string Website { get; set; }
        public string Email { get; set; }
        public string DeliveryModeStr { get; set; }
        public int UKPRN { get; set; }
        public int ProviderId { get; set; }
        public int? Radius { get; set; }
        public List<int> DeliveryModes
        {
            get
            {
                return DeliveryModeStr.Split(',').Select(x => Convert.ToInt32(x)).ToList();
            }
        }
    }

    [Serializable()]
    public class ResultMessage
    {
        public int ApprenticeshipID { get; set; }
        public string Status { get; set; }
        public string Message { get; set; }
    }

    public class ApprenticeshipDTO
    {
        public string id { get; set; }
        public int ApprenticeshipID { get; set; }
        public string ApprenticeshipTitle { get; set; }
        public string ProviderId { get; set; }
        public string StandardId { get; set; }
        public string FrameworkId { get; set; }
        public int? FrameworkCode { get; set; }
        public int ProviderUKPRN { get; set; }
        public int? StandardCode { get; set; }
        public int? PathWayCode { get; set; }
        public int? ProgType { get; set; }
        public int? Version { get; set; }
        public string MarketingInformation { get; set; }
        public string Url { get; set; }
        public string ContactEmail { get; set; }
        public string ContactTelephone { get; set; }
        public string ContactWebsite { get; set; }
        public int RecordStatusId { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public IList<ApprenticeshipLocationDTO> ApprenticeshipLocations { get; set; }
        public string NotionalNVQLevelv2 { get; set; }
        public int ApprenticeshipType { get; set; }
    }

    public class ApprenticeshipLocationDTO
    {
        public string Id { get; set; }
        public string VenueId { get; set; }
        public List<int> DeliveryModes { get; set; }
        public CourseDirectory.Models.Models.Apprenticeships.Address Address { get; set; }
        public int LocationId { get; set; }
        public string Name { get; set; }
        public int ProviderUKPRN { get; set; }
        public int ProviderId { get; set; }
        public int ApprenticeshipLocationType { get; set; }
        public int LocationType { get; set; }
        public List<string> Regions { get; set; }
        public int? Radius { get; set; }
        public int RecordStatus { get; set; }
        public DateTime CreatedDate { get; set; }
        public string CreatedBy { get; set; }
        public DateTime UpdatedDate { get; set; }
        public string UpdatedBy { get; set; }
        public string BulkUploadErrors { get; set; }
    }
}