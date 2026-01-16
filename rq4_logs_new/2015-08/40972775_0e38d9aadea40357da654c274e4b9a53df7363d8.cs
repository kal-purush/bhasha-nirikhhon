using System;
using System.Collections.Generic;
using System.Text;

namespace Base64Universal
{
    public class HelperMethods
    {
        /// <summary>
        /// Encode the input parameter to base64 and return it as a string.
        /// </summary>
        /// <param name="plainText">The string parameter that needs encoding</param>
        /// <returns></returns>
        public static string Base64Encode(string plainText)
        {
            if (!string.IsNullOrEmpty(plainText))
            {
                var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
                return System.Convert.ToBase64String(plainTextBytes);
            }
            return string.Empty;
        }

        /// <summary>
        /// Decode the input parameter from base64 and return it as a string.
        /// </summary>
        /// <param name="base64EncodedData">The string parameter that needs decoding</param>
        /// <returns></returns>
        public static string Base64Decode(string base64EncodedData)
        {
            if (!string.IsNullOrEmpty(base64EncodedData))
            {
                var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
                return Encoding.UTF8.GetString(base64EncodedBytes, 0, base64EncodedBytes.Length);
            }
            return string.Empty;
        }
    }
}