using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ProjectManagementSystem.BusinessLogicLayer
{
    internal class Cryptographer
    {
        public static string Encode(string value, string key)
        {
            byte[] bKey = Encoding.Default.GetBytes(key);
            using (var hmac = new HMACSHA256(bKey))
            {
                byte[] bValue = Encoding.Default.GetBytes(value);
                var bHash = hmac.ComputeHash(bValue);
                return BitConverter.ToString(bHash).Replace("-", string.Empty).ToLower();
            }
        }
    }
}