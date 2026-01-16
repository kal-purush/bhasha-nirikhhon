using System.Security.Cryptography;
using System.Text;

namespace EasyMicroservices.Utilities.Text
{
    /// <summary>
    /// helper of hash
    /// </summary>
    public static class HashHelper
    {
        /// <summary>
        /// Get SHA1 hash of string
        /// </summary>
        /// <param name="inputs"></param>
        /// <returns></returns>
        public static string GetSHA1Hash(this string[] inputs)
        {
            StringBuilder stringBuilder= new StringBuilder();
            foreach (string input in inputs)
            {
                stringBuilder.Append(GetSHA1Hash(input));
            }
            return GetSHA1Hash(stringBuilder.ToString());
        }

        /// <summary>
        /// Get SHA1 hash of string
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static string GetSHA1Hash(this string input)
        {
            using var sha1 = SHA1.Create();
            var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sb = new StringBuilder(hash.Length * 2);

            foreach (byte b in hash)
            {
                // can be "x2" if you want lowercase
                sb.Append(b.ToString("X2"));
            }

            return sb.ToString();
        }
    }
}