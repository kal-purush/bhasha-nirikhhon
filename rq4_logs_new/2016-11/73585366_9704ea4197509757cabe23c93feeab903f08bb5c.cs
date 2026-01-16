namespace RssReader.Extensions
{
    using System;

    public static class GuidExtensions
    {
        /// <summary>
        /// Get a 22-character, case-sensitive GUID as a string.
        /// </summary>
        public static string ToShortString(this Guid guid)
        {
            return Convert.ToBase64String(guid.ToByteArray())
                .Substring(0, 22)
                .Replace("/", "_")
                .Replace("+", "-");
        }
    }
}