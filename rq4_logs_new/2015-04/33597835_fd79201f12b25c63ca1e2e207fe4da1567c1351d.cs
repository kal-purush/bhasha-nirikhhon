using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace AppFabricTest
{
    public class CachePocUsingTags : CachePocBase
    {    
        public override void DoWork()
        {
            List<long> addList = new List<long>();
            List<long> getAndRemoveList = new List<long>();
            List<long> memUsageList = new List<long>();

            //List<DataCacheTag> datasetTags = new List<DataCacheTag>();

            //for (var i = 0; i < _datasetCount; i++)
            //{
            //    datasetTags.Add(new DataCacheTag("Dataset" + i));

            //    for (var j = 0; j < _dimensionCount; j++)
            //    {
            //        string key = datasetTags[i] + "DimensionName" + j;

            //        _stopWatch = Stopwatch.StartNew();

            //        var addResult = _myDefaultCache.Add(key, _data, new List<DataCacheTag>() { datasetTags[i] }, _regionName);

            //        _stopWatch.Stop();
            //        addList.Add(_stopWatch.ElapsedMilliseconds);

            //        if (addResult == null)
            //        {
            //            Console.WriteLine("**FAIL----->Add-Object did not add to cache - FAIL");
            //        }
            //    }
            //}

            //foreach (DataCacheTag dataCacheTag in datasetTags)
            //{
            //    long bytes1 = GC.GetTotalMemory(false);
            //    _stopWatch = Stopwatch.StartNew();

            //    foreach (var item in _myDefaultCache.GetObjectsByTag(dataCacheTag, _regionName))
            //    {
            //        _myDefaultCache.Remove(item.Key, _regionName);
            //    }

            //    _stopWatch.Stop();
            //    getAndRemoveList.Add(_stopWatch.ElapsedMilliseconds);
            //    memUsageList.Add(GC.GetTotalMemory(false) - bytes1);
            //}

            RecordStatistics(addList, getAndRemoveList, memUsageList);
        }
    }
}