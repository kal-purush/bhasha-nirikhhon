using Newtonsoft.Json;
using System.IO;

namespace F1Telemetry.Core.Util.Export
{
    public class JsonExporter : IExportEngine
    {
        public void WriteAll<T>(T objectToWrite, string path)
        {
            var serialized = JsonConvert.SerializeObject(objectToWrite);
            File.WriteAllText(path, serialized);
        }
    }

    public static class JSONExporterExtensions
    {
        public static IExportTarget AsJson(this IExporter exporter)
        {
            return exporter.As(new JsonExporter());
        }
    }
}