using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationServer.Caching;

namespace AppFabricTest
{
    class Program
    {
        private DataCacheFactory _myCacheFactory;
        private DataCache _myDefaultCache;
        private Stopwatch _stopWatch;
        private const string _regionName = "Region0";
        private readonly byte[] _data = new byte[100000];
        private const int _datasetCount = 1000;
        private const int _dimensionCount = 10;

        static void Main(string[] args)
        {
            Program program = new Program();
            program.PrepareClient();
            program.FlushCache();

            //program.NamedRegionUsingTags();
            program.NamedRegionForAllDatasetsUsingTags();
            //program.KeysDatasetCodesStoredInDb();

            Console.ReadKey();
        }

        private void FlushCache()
        {
            foreach (var region in _myDefaultCache.GetSystemRegions())
            {
                _myDefaultCache.ClearRegion(region);
            }

            for (var i = 0; i < _datasetCount; i++)
            {
                _myDefaultCache.RemoveRegion("Region" + i);
            }
        }

        private void PrepareClient()
        {
            List<DataCacheServerEndpoint> servers = new List<DataCacheServerEndpoint>(1)
            {
                new DataCacheServerEndpoint("localhost", 22233)
            };

            DataCacheFactoryConfiguration configuration = new DataCacheFactoryConfiguration
            {
                Servers = servers,
                LocalCacheProperties = new DataCacheLocalCacheProperties()
            };

            DataCacheClientLogManager.ChangeLogLevel(TraceLevel.Off);
            _myCacheFactory = new DataCacheFactory(configuration);
            _myDefaultCache = _myCacheFactory.GetCache("default");
        }

        private void NamedRegionUsingTags()
        {
            List<long> addList = new List<long>();
            List<long> getAndRemoveList = new List<long>();
            List<long> memUsageList = new List<long>();

            List<DataCacheTag> datasetTags = new List<DataCacheTag>();

            _myDefaultCache.CreateRegion(_regionName);

            for (var i = 0; i < _datasetCount; i++)
            {
                datasetTags.Add(new DataCacheTag("Dataset" + i));

                for (var j = 0; j < _dimensionCount; j++)
                {
                    string key = datasetTags[i] + "DimensionName" + j;

                    _stopWatch = Stopwatch.StartNew();
                    
                    var addResult = _myDefaultCache.Add(key, _data, new List<DataCacheTag>() { datasetTags[i] }, _regionName);
                    
                    _stopWatch.Stop();
                    addList.Add(_stopWatch.ElapsedMilliseconds);

                    if (addResult == null)
                    {
                        Console.WriteLine("**FAIL----->Add-Object did not add to cache - FAIL");
                    }
                }
            }
            
            foreach (DataCacheTag dataCacheTag in datasetTags)
            {
                long bytes1 = GC.GetTotalMemory(false);
                _stopWatch = Stopwatch.StartNew();

                foreach (var item in _myDefaultCache.GetObjectsByTag(dataCacheTag, _regionName))
                {
                    _myDefaultCache.Remove(item.Key, _regionName);
                }

                _stopWatch.Stop();
                getAndRemoveList.Add(_stopWatch.ElapsedMilliseconds);
                memUsageList.Add(GC.GetTotalMemory(false) - bytes1);
            }

            Console.WriteLine("\rRegion name - " + _regionName);
            Console.WriteLine("Dataset tag count - " + _datasetCount);
            Console.WriteLine("Dimension count - " + _dimensionCount);
            Console.WriteLine("Data length - " + _data.Length);
            Console.WriteLine("Max add ms - " + addList.Max());
            Console.WriteLine("Min add ms - " + addList.Min());
            Console.WriteLine("Avg add ms - " + addList.Average());
            Console.WriteLine("Sum add ms - " + addList.Sum());
            Console.WriteLine("Max get and remove ms - " + getAndRemoveList.Max());
            Console.WriteLine("Min get and remove ms - " + getAndRemoveList.Min());
            Console.WriteLine("Avg get and remove ms - " + getAndRemoveList.Average());
            Console.WriteLine("Sum get and remove ms - " + getAndRemoveList.Sum());
            Console.WriteLine("Max bytes used during a dataset delete - " + memUsageList.Max());
        }

        private void NamedRegionForAllDatasetsUsingTags()
        {
            List<long> addList = new List<long>();
            List<long> getAndRemoveList = new List<long>();
            List<long> memUsageList = new List<long>();

            List<string> datasetRegions = new List<string>();


            for (var i = 0; i < _datasetCount; i++)
            {
                string regionName = "Region" + i;
                datasetRegions.Add(regionName);
                _myDefaultCache.CreateRegion(regionName);

                for (var j = 0; j < _dimensionCount; j++)
                {
                    string key = datasetRegions[i] + "DimensionName" + j;

                    _stopWatch = Stopwatch.StartNew();

                    var addResult = _myDefaultCache.Add(key, _data, regionName);

                    _stopWatch.Stop();
                    addList.Add(_stopWatch.ElapsedMilliseconds);

                    if (addResult == null)
                    {
                        Console.WriteLine("**FAIL----->Add-Object did not add to cache - FAIL");
                    }
                }
            }

            foreach (string region in datasetRegions)
            {
                long bytes1 = GC.GetTotalMemory(false);
                _stopWatch = Stopwatch.StartNew();

                _myDefaultCache.ClearRegion(region);

                _stopWatch.Stop();
                getAndRemoveList.Add(_stopWatch.ElapsedMilliseconds);
                memUsageList.Add(GC.GetTotalMemory(false) - bytes1);
            }

            Console.WriteLine("Dataset region count - " + _datasetCount);
            Console.WriteLine("Dimension count - " + _dimensionCount);
            Console.WriteLine("Data length - " + _data.Length);
            Console.WriteLine("Max add ms - " + addList.Max());
            Console.WriteLine("Min add ms - " + addList.Min());
            Console.WriteLine("Avg add ms - " + addList.Average());
            Console.WriteLine("Sum add ms - " + addList.Sum());
            Console.WriteLine("Max get and remove ms - " + getAndRemoveList.Max());
            Console.WriteLine("Min get and remove ms - " + getAndRemoveList.Min());
            Console.WriteLine("Avg get and remove ms - " + getAndRemoveList.Average());
            Console.WriteLine("Sum get and remove ms - " + getAndRemoveList.Sum());
            Console.WriteLine("Max bytes used during a dataset delete - " + memUsageList.Max());
        }

        private void KeysDatasetCodesStoredInDb()
        {
            List<long> addList = new List<long>();
            List<long> getAndRemoveList = new List<long>();
            List<long> memUsageList = new List<long>();

            List<string> datasets = new List<string>();

            _myDefaultCache.CreateRegion(_regionName);

            Task.Factory.StartNew(() => InsertValue(datasets, addList)).Wait();
            
            foreach (var dataset in datasets)
            {
                long bytes1 = GC.GetTotalMemory(false);
                _stopWatch = Stopwatch.StartNew();

                foreach (var key in SelectAndDeleteFromDb(dataset))
                {
                    _myDefaultCache.Remove(key);
                }

                _stopWatch.Stop();
                memUsageList.Add(GC.GetTotalMemory(false) - bytes1);
                getAndRemoveList.Add(_stopWatch.ElapsedMilliseconds);
            }

            Console.WriteLine("\rRegion name - " + _regionName);
            Console.WriteLine("Dataset tag count - " + _datasetCount);
            Console.WriteLine("Dimension count - " + _dimensionCount);
            Console.WriteLine("Data length - " + _data.Length);
            Console.WriteLine("Max add ms - " + addList.Max());
            Console.WriteLine("Min add ms - " + addList.Min());
            Console.WriteLine("Avg add ms - " + addList.Average());
            Console.WriteLine("Sum add ms - " + addList.Sum());
            Console.WriteLine("Max get and remove ms - " + getAndRemoveList.Max());
            Console.WriteLine("Min get and remove ms - " + getAndRemoveList.Min());
            Console.WriteLine("Avg get and remove ms - " + getAndRemoveList.Average());
            Console.WriteLine("Sum get and remove ms - " + getAndRemoveList.Sum());
            Console.WriteLine("Max bytes used during a dataset delete - " + memUsageList.Max());
        }

        private void InsertValue(List<string> datasets, List<long> addList)
        {
            for (var i = 0; i < _datasetCount; i++)
            {
                string datasetCode = "Dataset" + i;
                datasets.Add(datasetCode);

                for (var j = 0; j < _dimensionCount; j++)
                {
                    string key = datasets[i] + "DimensionName" + j;

                    _stopWatch = Stopwatch.StartNew();

                    var addResult = _myDefaultCache.Add(key, _data);
                    Task.Run(() => InsertIntoDb(datasetCode, key));

                    _stopWatch.Stop();
                    addList.Add(_stopWatch.ElapsedMilliseconds);

                    if (addResult == null)
                    {
                        Console.WriteLine("**FAIL----->Add-Object did not add to cache - FAIL");
                    }
                }
            }
        }

        private async Task InsertIntoDb(string datasetCode, string cacheKey)
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["Dev"].ConnectionString))
            {
                await connection.OpenAsync();

                using (var command = new SqlCommand("INSERT INTO Test (DatasetCode, CacheKey) VALUES (@datasetCode, @cacheKey)", connection))
                {
                    command.Parameters.Add("@datasetCode", SqlDbType.VarChar, 50).Value = datasetCode;
                    command.Parameters.Add("@cacheKey", SqlDbType.VarChar, 50).Value = cacheKey;
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private IEnumerable<string> SelectAndDeleteFromDb(string datasetCode)
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["Dev"].ConnectionString))
            {
                connection.Open();

                using (var command = new SqlCommand("DELETE FROM Test OUTPUT DELETED.CacheKey WHERE DatasetCode = @datasetCode", connection))
                {
                    command.Parameters.Add("@datasetCode", SqlDbType.VarChar, 50).Value = datasetCode;
                    SqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        yield return reader["CacheKey"].ToString();
                    }
                }
            }
        }
    }
}