using CsvHelper;
using System.Globalization;
using System.IO;

namespace F1Telemetry.Core.Util.Export
{
    /// <summary>
    /// Convert class object to CSV format.
    /// </summary>
    /// <seealso cref="F1Telemetry.Core.Util.Export.IExportEngine" />
    public class CSVExporter : IExportEngine
    {
        public void WriteAll<T>(T objectToWrite, string path)
        {
            using (var writer = new StreamWriter(path))
            {
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords((System.Collections.IEnumerable)objectToWrite);
                }
            }
        }
    }

    public static class CSVExporterExtensions
    {
        public static IExportTarget AsCsv(this IExporter exporter)
        {
            exporter.As(new CSVExporter());

            return exporter;
        }
    }
}