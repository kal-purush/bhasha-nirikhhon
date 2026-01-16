using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using OSIsoft.Data;
using OSIsoft.Data.Reflection;
using OSIsoft.DataViews;
using OSIsoft.DataViews.Contracts;
using OSIsoft.DataViews.Data;
using OSIsoft.DataViews.Resolved;
using OSIsoft.Identity;

namespace DataViews
{
    public static class Program
    {
        private static IConfiguration _configuration;
        private static Exception _toThrow;

        public static void Main()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        public static async Task<bool> MainAsync(bool test = false)
        {
            ISdsMetadataService metadataService = null;
            IDataViewService dataviewService = null;

            #region settings

            // Sample Data Information
            var sampleTypeId1 = "Time_SampleType1";
            var sampleTypeId2 = "Time_SampleType2";
            var sampleStreamId1 = "dvTank2";
            var sampleStreamName1 = "Tank2";
            var sampleStreamDesc1 = "A stream to hold sample Pressure and Temperature events";
            var sampleStreamId2 = "dvTank100";
            var sampleStreamName2 = "Tank100";
            var sampleStreamDesc2 = "A stream to hold sample Pressure and Ambient Temperature events";
            var sampleFieldToConsolidateTo = "Temperature";
            var sampleFieldToConsolidate = "AmbientTemperature";

            // Data View Information
            var sampleDataViewId = "DataView_Sample";
            var sampleDataViewName = "DataView_Sample_Name";
            var sampleDataViewDescription = "A Sample Description that describes that this Data View is just used for our sample.";
            var sampleQueryId = "stream";
            var sampleQueryString = "dvTank*";
            TimeSpan sampleRange = new TimeSpan(1, 0, 0); // range of one hour
            TimeSpan sampleInterval = new TimeSpan(0, 20, 0); // timespan of twenty minutes
            #endregion // settings

            try
            {
                #region configurationSettings

                _configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .Build();

                var tenantId = _configuration["TenantId"];
                var namespaceId = _configuration["NamespaceId"];
                var resource = _configuration["Resource"];
                var clientId = _configuration["ClientId"];
                var clientKey = _configuration["ClientKey"];
                var apiVersion = _configuration["ApiVersion"];

                (_configuration as ConfigurationRoot).Dispose();
                var uriResource = new Uri(resource);
                #endregion // configurationSettings

                // Step 1 - Authenticate Against OCS
                #region step1
                Console.WriteLine("Step 1: Authenticate Against OCS");

                SdsService sdsService = new SdsService(new Uri(resource), new AuthenticationHandler(uriResource, clientId, clientKey));
                metadataService = sdsService.GetMetadataService(tenantId, namespaceId);
                var dataService = sdsService.GetDataService(tenantId, namespaceId);
                var tableService = sdsService.GetTableService(tenantId, namespaceId);

                var dataviewServiceFactory = new DataViewServiceFactory(new Uri(resource), new AuthenticationHandler(uriResource, clientId, clientKey));
                dataviewService = dataviewServiceFactory.GetDataViewService(tenantId, namespaceId);
                #endregion // step1

                // Step 2 - Create Types, Streams, and Data
                #region step2
                Console.WriteLine("Step 2: Create types, streams, and data");

                // create both sample types
                SdsType sampleType1 = SdsTypeBuilder.CreateSdsType<SampleType1>();
                sampleType1.Id = sampleTypeId1;
                sampleType1 = await metadataService.GetOrCreateTypeAsync(sampleType1).ConfigureAwait(false);

                SdsType sampleType2 = SdsTypeBuilder.CreateSdsType<SampleType2>();
                sampleType2.Id = sampleTypeId2;
                sampleType2 = await metadataService.GetOrCreateTypeAsync(sampleType2).ConfigureAwait(false);

                // create streams
                var sampleStream1 = new SdsStream
                {
                    Id = sampleStreamId1,
                    Name = sampleStreamName1,
                    TypeId = sampleTypeId1,
                    Description = sampleStreamDesc1,
                };
                sampleStream1 = await metadataService.GetOrCreateStreamAsync(sampleStream1).ConfigureAwait(false);

                var sampleStream2 = new SdsStream
                {
                    Id = sampleStreamId2,
                    Name = sampleStreamName2,
                    TypeId = sampleTypeId2,
                    Description = sampleStreamDesc2,
                };
                sampleStream2 = await metadataService.GetOrCreateStreamAsync(sampleStream2).ConfigureAwait(false);

                // create data
                DateTime sampleEndTime = DateTime.Now;
                DateTime sampleStartTime = sampleEndTime.AddSeconds(-sampleRange.TotalSeconds);

                List<SampleType1> sampleValues1 = new List<SampleType1>();
                List<SampleType2> sampleValues2 = new List<SampleType2>();

                var rand = new Random();
                double pressureUpperLimit = 100;
                double pressureLowerLimit = 0;
                double tempUpperLimit = 70;
                double tempLowerLimit = 50;
                int dataFrequency = 120; // does not need to match data view sampling interval

                for (double offsetSeconds = 0; offsetSeconds <= sampleRange.TotalSeconds; offsetSeconds += dataFrequency)
                {
                    SampleType1 val1 = new SampleType1
                    {
                        Pressure = (rand.NextDouble() * (pressureUpperLimit - pressureLowerLimit)) + pressureLowerLimit,
                        Temperature = (rand.NextDouble() * (tempUpperLimit - tempLowerLimit)) + tempLowerLimit,
                        Time = sampleStartTime.AddSeconds(offsetSeconds),
                    };

                    SampleType2 val2 = new SampleType2
                    {
                        Pressure = (rand.NextDouble() * (pressureUpperLimit - pressureLowerLimit)) + pressureLowerLimit,
                        AmbientTemperature = (rand.NextDouble() * (tempUpperLimit - tempLowerLimit)) + tempLowerLimit,
                        Time = sampleStartTime.AddSeconds(offsetSeconds),
                    };

                    sampleValues1.Add(val1);
                    sampleValues2.Add(val2);
                }

                // upload data
                await dataService.InsertValuesAsync(sampleStreamId1, sampleValues1).ConfigureAwait(false);
                await dataService.InsertValuesAsync(sampleStreamId2, sampleValues2).ConfigureAwait(false);

                #endregion //step2

                // Step 3 - Create a Data View 
                #region step3
                Console.WriteLine("Step 3: Create a Data View");
                var dataView = new DataView
                {
                    Id = sampleDataViewId,
                    Name = sampleDataViewName,
                    Description = sampleDataViewDescription,
                };
                dataView = await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);
                #endregion //step3
                
                // Step 4 - Retrieve the Data View
                #region step4
                Console.WriteLine("Step 4: Retrieve the Data View");

                dataView = await dataviewService.GetDataViewAsync(sampleDataViewId).ConfigureAwait(false);
                Console.WriteLine();
                Console.WriteLine($"Retrieved Data View:");
                Console.WriteLine($"ID: {dataView.Id}, Name: {dataView.Name}, Description: {dataView.Description}");
                Console.WriteLine();
                #endregion //step4

                // Step 5 - Add a Query for Data Items
                #region step5
                Console.WriteLine("Step 5: Add a Query for Data Items");

                var query = new Query
                {
                    Id = sampleQueryId,
                    Value = sampleQueryString,
                    Kind = DataItemResourceType.Stream,
                };

                dataView.Queries.Add(query);

                await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);
                #endregion //step5

                // Step 6 - View Items Found by the Query
                #region step6
                Console.WriteLine("Step 6: View Items Found by the Query");

                ResolvedItems<DataItem> resolvedDataItems = await dataviewService.GetDataItemsAsync(dataView.Id, query.Id).ConfigureAwait(false);
                ResolvedItems<DataItem> ineligibleDataItems = await dataviewService.GetIneligibleDataItemsAsync(dataView.Id, query.Id).ConfigureAwait(false);

                Console.WriteLine();
                Console.WriteLine($"Resolved data items for query {query.Id}:");
                foreach (var dataItem in resolvedDataItems.Items)
                {
                    Console.WriteLine($"Name: {dataItem.Name}; ID: {dataItem.Id}");
                }

                Console.WriteLine();

                Console.WriteLine($"Ineligible data items for query {query.Id}:");
                foreach (var dataItem in ineligibleDataItems.Items)
                {
                    Console.WriteLine($"Name: {dataItem.Name}; ID: {dataItem.Id}");
                }

                Console.WriteLine();

                #endregion //step6

                // Step 7 - View Fields Available to Include in the Data View
                #region step7
                Console.WriteLine("Step 7: View Fields Available to Include in the Data View");

                ResolvedItems<FieldSet> availableFields = await dataviewService.GetAvailableFieldSetsAsync(dataView.Id).ConfigureAwait(false);

                Console.WriteLine();
                Console.WriteLine($"Available fields for data view {dataView.Name}:");
                foreach (var fieldset in availableFields.Items)
                {
                    Console.WriteLine($"Fieldset:");
                    Console.WriteLine($"QueryId: {fieldset.QueryId}");
                    Console.WriteLine($"Data Fields: ");
                    foreach (var datafield in fieldset.DataFields)
                    {
                        Console.Write($"Label: {datafield.Label}");
                        Console.Write($", Source: {datafield.Source}");
                        foreach (var key in datafield.Keys)
                        {
                            Console.Write($", Key: {key}");
                        }

                        Console.Write('\n');
                    }
                }
                
                Console.WriteLine();
                #endregion //step7

                // Step 8 - Include Some of the Available Fields
                #region step8
                Console.WriteLine("Step 8: Include Some of the Available Fields");

                foreach (var field in availableFields.Items)
                {
                    dataView.DataFieldSets.Add(field);
                }

                await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);

                var values = dataviewService.GetDataInterpolatedAsync(
                    dataView.Id,
                    OutputFormat.Default,
                    sampleStartTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleEndTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleInterval.ToString(),
                    null,
                    CacheBehavior.Refresh,
                    default);

                await foreach (var value in values)
                {
                    Console.WriteLine(value);
                }

                #endregion //step8

                // Step 9 - Group the Data View
                #region step9
                Console.WriteLine("Step 9: Group the Data View");

                dataView.GroupingFields.Add(new Field
                {
                    Source = FieldSource.Id,
                    Label = "{IdentifyingValue} {Key}",
                });

                await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);

                values = dataviewService.GetDataInterpolatedAsync(
                    dataView.Id,
                    OutputFormat.Default,
                    sampleStartTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleEndTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleInterval.ToString(),
                    null,
                    CacheBehavior.Refresh,
                    default);

                await foreach (var value in values)
                {
                    Console.WriteLine(value);
                }

                #endregion //step9

                // Step 10 - Identify Data Items
                #region step10
                Console.WriteLine("Step 10: Identify Data Items");

                foreach (var thisFieldSet in dataView.DataFieldSets.ToList())
                {
                    thisFieldSet.IdentifyingField = new Field
                    {
                        Source = FieldSource.Id,
                        Label = "{IdentifyingValue} {Key}",
                    };
                }

                await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);

                values = dataviewService.GetDataInterpolatedAsync(
                    dataView.Id,
                    OutputFormat.Default,
                    sampleStartTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleEndTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleInterval.ToString(),
                    null,
                    CacheBehavior.Refresh,
                    default);

                await foreach (var value in values)
                {
                    Console.WriteLine(value);
                }

                #endregion //step10

                // Step 11 - Consolidate Data Fields
                #region step11
                Console.WriteLine("Step 11: Consolidate Data Fields");

                var fieldSet = dataView.DataFieldSets.Single(a => a.QueryId == sampleQueryId);
                fieldSet.DataFields.Remove(fieldSet.DataFields.Single(a => a.Keys.Contains(sampleFieldToConsolidate)));

                var consolidatingField = fieldSet.DataFields.Single(a => a.Keys.Contains(sampleFieldToConsolidateTo));
                consolidatingField.Keys.Add(sampleFieldToConsolidate);

                await dataviewService.CreateOrUpdateDataViewAsync(dataView).ConfigureAwait(false);

                values = dataviewService.GetDataInterpolatedAsync(
                    dataView.Id,
                    OutputFormat.Default,
                    sampleStartTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleEndTime.ToUniversalTime().ToString(CultureInfo.InvariantCulture),
                    sampleInterval.ToString(),
                    null,
                    CacheBehavior.Refresh,
                    default);

                await foreach (var value in values)
                {
                    Console.WriteLine(value);
                }

                #endregion //step11
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                _toThrow = ex;
                throw;
            }
            finally
            {
                // Step 12 - Delete Sample Objects from OCS
                #region step12

                if (dataviewService != null)
                {
                    // Delete the data view
                    await RunInTryCatch(dataviewService.DeleteDataViewAsync, sampleDataViewId).ConfigureAwait(false);

                    Thread.Sleep(10); // slight rest here for consistency

                    // Check Delete
                    await RunInTryCatchExpectException(dataviewService.GetDataViewAsync, sampleDataViewId).ConfigureAwait(false);
                }

                if (metadataService != null)
                {
                    // Delete everything
                    Console.WriteLine("Step 12: Delete Sample Objects from OCS");
                    await RunInTryCatch(metadataService.DeleteStreamAsync, sampleStreamId1).ConfigureAwait(false);
                    await RunInTryCatch(metadataService.DeleteStreamAsync, sampleStreamId2).ConfigureAwait(false);
                    await RunInTryCatch(metadataService.DeleteTypeAsync, sampleTypeId1).ConfigureAwait(false);
                    await RunInTryCatch(metadataService.DeleteTypeAsync, sampleTypeId2).ConfigureAwait(false);

                    Thread.Sleep(10); // slight rest here for consistency

                    // Check Deletes
                    await RunInTryCatchExpectException(metadataService.GetStreamAsync, sampleStreamId1).ConfigureAwait(false);
                    await RunInTryCatchExpectException(metadataService.GetStreamAsync, sampleStreamId2).ConfigureAwait(false);
                    await RunInTryCatchExpectException(metadataService.GetTypeAsync, sampleTypeId1).ConfigureAwait(false);
                    await RunInTryCatchExpectException(metadataService.GetTypeAsync, sampleTypeId2).ConfigureAwait(false);
                }

                #endregion //step12
            }

            if (test && _toThrow != null)
                throw _toThrow;

            return _toThrow == null;
        }

        /// <summary>
        /// Use this to run a method that you don't want to stop the program if there is an exception
        /// </summary>
        /// <param name="methodToRun">The method to run.</param>
        /// <param name="value">The value to put into the method to run</param>
        private static async Task RunInTryCatch(Func<string, Task> methodToRun, string value)
        {
            try
            {
                await methodToRun(value).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Got error in {methodToRun.Method.Name} with value {value} but continued on: {ex.Message}");
                if (_toThrow == null)
                {
                    _toThrow = ex;
                }
            }
        }

        /// <summary>
        /// Use this to run a method that you don't want to stop the program if there is an exception, and you expect an exception
        /// </summary>
        /// <param name="methodToRun">The method to run.</param>
        /// <param name="value">The value to put into the method to run</param>
        private static async Task RunInTryCatchExpectException(Func<string, Task> methodToRun, string value)
        {
            try
            {
                await methodToRun(value).ConfigureAwait(false);

                Console.WriteLine($"Got error.  Expected {methodToRun.Method.Name} with value {value} to throw an error but it did not.");
            }
            catch
            {
            }
        }
    }
}