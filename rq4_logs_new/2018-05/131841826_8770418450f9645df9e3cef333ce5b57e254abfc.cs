using System;
using System.Security.Cryptography;
using System.Text;

namespace Reppertum {
    public class Cryptography {
        public static string Sha256(string str) {
            SHA256Managed crypt = new SHA256Managed();
            StringBuilder hash = new StringBuilder();
            Byte[] crypto = crypt.ComputeHash(Encoding.UTF8.GetBytes(str));
            foreach (Byte theByte in crypto)
            {
                hash.Append(theByte.ToString("x2"));
            }
            return hash.ToString();
        }
    }
}