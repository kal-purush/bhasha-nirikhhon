using MixTelematics.Models;
using MixTelematics.Utilities;
using System.Text.Json;

namespace MixTelematics.Services
{
    public class VehiclePositionCacheService
    {
        private const int NumFiles = 40;
        private const string outputDirectory = @"..\..\..\SortedDataInput";
        public List<VehiclePosition> ReadCachedVehiclePositions()
        {
            var tasks = new Task<List<VehiclePosition>>[NumFiles];
            int i = 0;
            foreach(var file in Directory.EnumerateFiles(outputDirectory).OrderBy(x => x))
            {
                tasks[i] = Task.Run(() => ReadCachedVehiclePositions(file));
                i++;
            }
            Task.WaitAll(tasks);
            var results = tasks.SelectMany(x => x.Result).ToList();
            return results;
        }
        public async Task CacheVehiclePositions()
        {
            if (Directory.GetFiles(outputDirectory).Any())
            {
                return;
            }
            var path = @"..\..\..\VehiclePositions.dat";
            var vehiclePositions = await Task.Run(() => FileUtilityHelper.ReadBinaryDataFile(path)).ContinueWith(x =>
            {
                return x.Result;
            });

            var vehiclePositionsArray = vehiclePositions.ToArray(); 
            Array.Sort(vehiclePositions.Select(x => x.Latitude).ToArray(), vehiclePositionsArray);

            SplitRecords(vehiclePositionsArray);
        }
        private List<VehiclePosition> ReadCachedVehiclePositions(string path)
        {
            var fileDataText = File.ReadAllText(path);
            var vehicles = JsonSerializer.Deserialize<List<VehiclePosition>>(fileDataText);
            
            return vehicles;
        }
            
        private void SplitRecords(VehiclePosition[] vehiclePositions)
        {
            int recordsPerFile = vehiclePositions.Length / NumFiles;
        
            // Write records to individual output files
            for (int i = 0; i < NumFiles; i++)
            {
                string outputFilePath = Path.Combine(outputDirectory, $"output_file_{i}.json");
                using (StreamWriter writer = new StreamWriter(outputFilePath))
                {
                    int startIdx = i * recordsPerFile;
                    int endIdx = startIdx + recordsPerFile;
                    if (i == NumFiles - 1) // Handle the last file to include any remaining records
                    {
                        endIdx = vehiclePositions.Length;
                    }
                    var records = vehiclePositions[startIdx..endIdx];
                    writer.WriteLine(JsonSerializer.Serialize(records));
                }
            }


        }
    }
}