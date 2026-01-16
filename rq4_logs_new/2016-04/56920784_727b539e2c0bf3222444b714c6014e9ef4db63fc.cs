using System;
using System.Collections.Generic;
using Couchbase;
using Couchbase.Configuration.Client;
using GeoJSON.Net.Geometry;
using SpaceHerders.Models;

namespace CouchbaseTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = new ClientConfiguration
            {
                Servers = new List<Uri> { new Uri("http://104.236.220.12:8091/pools") },
                BucketConfigs = new Dictionary<string, BucketConfiguration>
                {
                    ["geodb"] = new BucketConfiguration
                    {
                        BucketName = "geodb",
                        Password = "123456",
                    }
                }

            };

            var testDoc = new CrowdsourcedPlace
            {
                CreationTime = DateTime.UtcNow,
                CreatorId = Guid.NewGuid(),
                CrowdsourcedPlaceType = CrowdsourcedPlaceType.Pasture,
                Description = "desc",
                Point = new Point(new GeographicPosition(0.1d, 0.2d)),
                PlaceId = Guid.NewGuid()
            };

            using (var cluster = new Cluster(config))
            using (var bucket = cluster.OpenBucket("geodb"))
            {
                var res = bucket.InsertAsync(new Document<CrowdsourcedPlace>() {Content = testDoc, Id = testDoc.PlaceId.ToString()}).Result;
            }
        }
    }
}