using System.IO;

namespace VersionNumberIncrementer.Domain
{
    public class FileService : IFileService
    {
        private static string ProductInfoFileLocation => "../../ProductInfo.txt";

        public void WriteVersionNumberToFile(Release release)
        {
            File.WriteAllText(ProductInfoFileLocation, release.Version.ToString());
        }

        public string ReadVersionNumberFromFile()
        {
            return File.ReadAllText(ProductInfoFileLocation).Trim();
        }
    }
}