using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Dfc.ProviderPortal.TribalExporter.Models;

namespace Dfc.ProviderPortal.TribalExporter.Helpers
{
    public static class RegionLookup
    {
        private static Dictionary<int, string> s_lookup;

        public static IReadOnlyCollection<string> FindRegions(int venueLocationId)
        {
            var lookup = LazyInitializer.EnsureInitialized(ref s_lookup, LoadLookupFile);

            lookup.TryGetValue(venueLocationId, out var regionId);
            
            if (regionId == default)
            {
                return Array.Empty<string>();
            }
            else
            {
                // If result is a region (not a sub-region) we need to expand to all the sub regions
                var region = RegionInfo.All.SingleOrDefault(r => r.Id == regionId);
                if (region != null)
                {
                    return region.SubRegions.Select(sr => sr.Id).ToList();
                }
                else
                {
                    return new[] { regionId };
                }
            }

            Dictionary<int, string> LoadLookupFile()
            {
                var results = new Dictionary<int, string>();

                var lookupFileResourceName = "Dfc.ProviderPortal.TribalExporter.VenueLookup.txt";
                using (var lookupFile = typeof(RegionLookup).Assembly.GetManifestResourceStream(lookupFileResourceName))
                using (var reader = new StreamReader(lookupFile))
                {
                    string line;

                    while ((line = reader.ReadLine()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            continue;
                        }

                        var parts = line.Split("\t,".ToCharArray());
                        var vlId = int.Parse(parts[0]);
                        var region = parts[1];

                        results.Add(vlId, region);
                    }
                }

                return results;
            }
        }
    }
}