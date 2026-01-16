using System;
using System.IO;

namespace VersionNumberIncrementer.Domain
{
    public class ReleaseService : IReleaseService
    {
        private static string ProductInfoFileLocation => "../../ProductInfo.txt";

        public void WriteVersionNumberToFile(Release release)
        {
            File.WriteAllText(ProductInfoFileLocation, release.VersionNumber);
        }

        public string ReadVersionNumberFromFile()
        {
            return File.ReadAllText(ProductInfoFileLocation);
        }

        IVersionNumberStrategy IReleaseService.GetVersionStrategyForReleaseType(Release.ReleaseTypeEnum releaseType)
        {
            switch (releaseType)
            {
                case Release.ReleaseTypeEnum.BugFix:
                    return new MinorVersionNumberStrategy();
                case Release.ReleaseTypeEnum.Feature:
                    return new MajorVersionNumberStrategy();
                default:
                    throw new ArgumentOutOfRangeException(nameof(releaseType), releaseType,
                        "Unknown release type entered.");
            }
        }
    }
}