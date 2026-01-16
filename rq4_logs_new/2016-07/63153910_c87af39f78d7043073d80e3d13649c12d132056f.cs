using System;

namespace VersionNumberIncrementer.Domain
{
    public class ApplicationVersion
    {
        public enum ReleaseTypeEnum
        {
            Feature,
            BugFix
        }

        public Version Version { get; }

        public ApplicationVersion(string versionNumber)
        {
            Version = ParseVersion(versionNumber);
        }

        private ApplicationVersion(Version version)
        {
            Version = version;
        }

        private static Version ParseVersion(string input)
        {
            Version version;

            if (Version.TryParse(input, out version))
                return version;

            throw new FormatException("Invalid version number entered.");
        }

        public ApplicationVersion IncrementVersion(ReleaseTypeEnum releaseType)
        {
            try
            {
                var incrementedVersion = IncrementVersionImplementation(releaseType);
                return new ApplicationVersion(incrementedVersion);
            }
            catch (ArgumentOutOfRangeException exception)
            {
                throw new InvalidOperationException("Cannot increment version number for release as value would be out of range", exception);
            }  
        }

        private Version IncrementVersionImplementation(ReleaseTypeEnum releaseType)
        {
            switch (releaseType)
            {
                case ReleaseTypeEnum.BugFix:
                    return BugFixIncrement();
                case ReleaseTypeEnum.Feature:
                    return FeatureIncrement();
                default:
                    throw new ArgumentOutOfRangeException(nameof(releaseType), releaseType,
                        "Unknown release type entered.");
            }
        }

        private Version BugFixIncrement()
        {
            return new Version(Version.Major, Version.Minor, Version.Build, Version.Revision + 1);
        }

        private Version FeatureIncrement()
        {
            return new Version(Version.Major, Version.Minor, Version.Build + 1, 0);
        }
    }
}