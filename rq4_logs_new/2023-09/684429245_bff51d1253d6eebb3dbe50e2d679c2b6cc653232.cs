using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace ZOOMValidation.Extensions
{
    internal static class StringExtensions
    {
        internal static string ToSha1Hash(this string input)
        {
            using (SHA1 sha1 = SHA1.Create())
            {
                byte[] inputBytes = Encoding.UTF8.GetBytes(input);
                byte[] hashBytes = sha1.ComputeHash(inputBytes);
                return BitConverter.ToString(hashBytes).Replace("-", "").ToUpper();
            }
        }
    }
}