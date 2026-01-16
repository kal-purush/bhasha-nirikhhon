using System;
using System.Text;

namespace proactima.jsonobject
{
    internal static class EncodingHelper
    {
        internal static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.Unicode.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        internal static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return Encoding.Unicode.GetString(base64EncodedBytes);
        }
    }
}