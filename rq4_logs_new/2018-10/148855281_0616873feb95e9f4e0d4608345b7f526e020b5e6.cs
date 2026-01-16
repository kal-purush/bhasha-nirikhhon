using System.Collections.Generic;
using System.IO;

namespace SymbioticTS.Core.IOAbstractions
{
    internal class AuditingFileSink : IFileSink
    {
        private readonly HashSet<string> filesCreated = new HashSet<string>();

        private readonly IFileSink fileSink;

        /// <summary>
        /// Gets the files created.
        /// </summary>
        public IReadOnlyCollection<string> FilesCreated => this.filesCreated;

        /// <summary>
        /// Initializes a new instance of the <see cref="AuditingFileSink"/> class.
        /// </summary>
        /// <param name="fileSink">The file sink.</param>
        public AuditingFileSink(IFileSink fileSink)
        {
            this.fileSink = fileSink;
        }

        /// <summary>
        /// Creates a file.
        /// </summary>
        /// <param name="fileName">The name of the file.</param>
        /// <returns>A <see cref="T:System.IO.Stream" />.</returns>
        public Stream CreateFile(string fileName)
        {
            return this.CreateFile(fileName, out string filePath);
        }

        /// <summary>
        /// Creates a file.
        /// </summary>
        /// <param name="fileName">The name of the file.</param>
        /// <param name="filePath">The file path created.</param>
        /// <returns>A <see cref="T:System.IO.Stream" />.</returns>
        public Stream CreateFile(string fileName, out string filePath)
        {
            Stream result = this.fileSink.CreateFile(fileName, out filePath);

            this.filesCreated.Add(filePath);

            return result;
        }

        /// <summary>
        /// Gets an <see cref="T:SymbioticTS.Core.IOAbstractions.IFileSource" /> representing the contents of the <see cref="T:SymbioticTS.Core.IOAbstractions.IFileSink" />.
        /// </summary>
        /// <returns>An <see cref="T:SymbioticTS.Core.IOAbstractions.IFileSource" />.</returns>
        public IFileSource ToSource()
        {
            return this.fileSink.ToSource();
        }
    }
}