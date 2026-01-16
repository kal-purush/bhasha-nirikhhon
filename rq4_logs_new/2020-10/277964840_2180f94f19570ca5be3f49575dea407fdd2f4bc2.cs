namespace F1Telemetry.Core.Util.Export
{
    public interface IExporter : IExportTarget
    {
        /// <summary>
        /// Specify the object to export.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objectToExport">The object to export.</param>
        /// <returns></returns>
        IExporter Export<T>(T objectToExport);

        /// <summary>
        /// Set the designated format.
        /// </summary>
        /// <param name="engine">The engine.</param>
        /// <returns></returns>
        IExportTarget As(IExportEngine exporterEngine);
    }

    /// <summary>
    /// The engine that converts object into a different format.
    /// </summary>
    public interface IExportEngine
    {
        /// <summary>
        /// Writes all data to disk.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objectToWrite">The object to write.</param>
        /// <param name="path">The path.</param>
        void WriteAll<T>(T objectToWrite, string path);
    }

    /// <summary>
    /// This interface specifies the target/destination of the export.
    /// </summary>
    public interface IExportTarget
    {
        void ToFile(string path);
    }
}