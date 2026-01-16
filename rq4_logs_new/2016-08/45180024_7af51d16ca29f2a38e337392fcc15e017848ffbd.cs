using System;
using System.Text.RegularExpressions;

namespace DbUp.Engine
{
    /// <summary>
    /// Stores a script version. Capable of storing a Major and Minor version (eg Major.Minor)
    /// </summary>
    public class ScriptVersion: IComparable<ScriptVersion>
    {
        public int MajorVersion { get; set; }
        public int? MinorVersion { get; set; }

        public int CompareTo(ScriptVersion other)
        {
            if (other == null) return 1;
            if (MajorVersion == other.MajorVersion)
            {
                if (MinorVersion == other.MinorVersion)
                {
                    return 0;
                }

                if (MinorVersion < other.MinorVersion)
                {
                    return -1;
                }
                return 1;
            }

            if (MajorVersion < other.MajorVersion)
            {
                return -1;
            }

            return 1;

        }

        public static bool operator <(ScriptVersion v1, ScriptVersion v2)
        {
            return v1.CompareTo(v2) < 0;
        }

        public static bool operator >(ScriptVersion v1, ScriptVersion v2)
        {
            return v1.CompareTo(v2) > 0;
        }

        public static bool operator <=(ScriptVersion v1, ScriptVersion v2)
        {
            return v1.CompareTo(v2) <= 0;
        }

        public static bool operator >=(ScriptVersion v1, ScriptVersion v2)
        {
            return v1.CompareTo(v2) >= 0;
        }

        public static bool TryGetVersion(string versionString, out ScriptVersion scriptVersion)
        {
            var regex = new Regex(@"(^\d+)\.?(\d*)");
            var match = regex.Match(versionString);
            if (match.Success)
            {
                scriptVersion = new ScriptVersion();
                int majorVersion;
                if (Int32.TryParse(match.Groups[1].Value, out majorVersion))
                {
                    scriptVersion.MajorVersion = majorVersion;
                }
                if (match.Groups.Count > 1)
                {
                    int minorVersion;
                    if (Int32.TryParse(match.Groups[2].Value, out minorVersion))
                    {
                        scriptVersion.MinorVersion = minorVersion;
                    }
                }
                return true;
            }

            scriptVersion = null;
            return false; ;
        }

    }
}