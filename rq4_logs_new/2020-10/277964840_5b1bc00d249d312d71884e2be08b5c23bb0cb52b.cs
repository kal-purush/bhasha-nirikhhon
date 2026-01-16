using System;

namespace F1Telemetry.Core.Util.Export
{
    /// <summary>
    /// This class handles exporting/converting data into a different format.
    /// </summary>
    /// <seealso cref="F1Telemetry.Core.Util.Export.IExporter" />
    public class Exporter : IExporter
    {
        private IExportEngine exportEngine;
        private object objectToExport;

        public IExportTarget As(IExportEngine engine)
        {
            exportEngine = engine;

            return this;
        }

        public IExporter Export<T>(T objectToExport)
        {
            this.objectToExport = objectToExport;

            return this;
        }

        public void ToFile(string path)
        {
            if (exportEngine is null)
            {
                throw new InvalidOperationException("Export engine is not specified.");
            }

            exportEngine.WriteAll(objectToExport, path);
        }
    }
}