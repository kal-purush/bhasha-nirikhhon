using System;
using System.IO;

namespace EbSoft.Warehouse.SDK.UnitTests.Extensions
{
    public class FileContent
    {
        private readonly string _pathToContent;

        public FileContent(string pathToContent)
        {
            _pathToContent = pathToContent;
        }

        public override string ToString()
        {
            return File.ReadAllText(_pathToContent);
        }

        public FileInfo Info
        {
            get
            {
                return new FileInfo(_pathToContent);
            }
        }
    }
}