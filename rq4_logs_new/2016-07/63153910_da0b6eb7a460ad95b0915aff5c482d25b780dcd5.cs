using System;
using System.Text.RegularExpressions;

namespace VersionNumberIncrementer.Domain
{
    public class Release
    {
        public enum ReleaseTypeEnum
        {
            Feature = 1,
            BugFix = 2
        }

        protected int _majorVersion;
        protected int _minorVersion;
        protected int _placeHolder1Version;
        protected int _placeHolder2Version;
        protected int _releaseType;
        protected string _versionNumber;

        public Release(string versionNumber, int releaseType)
        {
            _versionNumber = versionNumber;
            _releaseType = releaseType;

            var regex = new Regex(@"^(?<PlaceHolder1>\d+)\.(?<PlaceHolder2>\d+)\.(?<Major>\d+)\.(?<Minor>\d+)$");
            var match = regex.Match(versionNumber);

            if (!match.Success)
                return;

            _placeHolder1Version = Convert.ToInt32(match.Groups["PlaceHolder1"].Value);
            _placeHolder2Version = Convert.ToInt32(match.Groups["PlaceHolder2"].Value);
            _majorVersion = Convert.ToInt32(match.Groups["Major"].Value);
            _minorVersion = Convert.ToInt32(match.Groups["Minor"].Value);
        }

        public string VersionNumber
        {
            get
            {
                return $"{PlaceHolder1Version}.{PlaceHolder2Version}.{MajorVersion}.{MinorVersion}";
            }

            set { _versionNumber = value; }
        }

        public ReleaseTypeEnum ReleaseType
        {
            get { return (ReleaseTypeEnum) _releaseType; }
            set { _releaseType = (int) value; }
        }

        public int PlaceHolder1Version
        {
            get { return _placeHolder1Version; }
            set { _placeHolder1Version = value; }
        }

        public int PlaceHolder2Version
        {
            get { return _placeHolder2Version; }
            set { _placeHolder2Version = value; }
        }

        public int MajorVersion
        {
            get { return _majorVersion; }
            set { _majorVersion = value; }
        }

        public int MinorVersion
        {
            get { return _minorVersion; }
            set { _minorVersion = value; }
        }
    }
}