namespace Wharrgarbl.CoreExtensions
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;

    public static class ReflectionEx
    {
        public static AssemblyName GetAssemblyName(this FileInfo file)
        {
            return AssemblyName.GetAssemblyName(file.FullName);
        }

        public static string GetThreePartVersion(this Version version)
        {
            return string.Concat(
                version.Major.ToString(CultureInfo.InvariantCulture),
                ".",
                version.Minor.ToString(CultureInfo.InvariantCulture),
                ".",
                version.Build.ToString(CultureInfo.InvariantCulture));
        }

        public static string ConvertToSemVer(this Version version, string preRelease = null, string metadata = null)
        {
            var result = version.GetThreePartVersion();

            if (!string.IsNullOrEmpty(preRelease))
            {
                result = preRelease.StartsWith("-")
                    ? (result + preRelease)
                    : (result + "-" + preRelease);
            }

            if (!string.IsNullOrEmpty(metadata))
            {
                result = metadata.StartsWith("+")
                    ? (result + metadata)
                    : (result + "+" + metadata);
            }

            return result;
        }
    }
}