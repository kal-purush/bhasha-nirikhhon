using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSAAlgorythm.Data
{
    public class FileDataProvider : IDataProvider
    {
        public string FilePath { get; }
        public FileDataProvider(string filePath)
        {
            if(filePath == null)
            {
                throw new ArgumentNullException(nameof(filePath));
            }
            if (!File.Exists(filePath))
            {
                throw new ArgumentException("Provided file path is not reachable");
            }
            FilePath = filePath;
        }
        public byte[] GetData()
        {
            if (!File.Exists(FilePath))
            {
                throw new ArgumentException("Provided file path is not reachable");
            }

            return File.ReadAllBytes(FilePath);
        }
    }
}