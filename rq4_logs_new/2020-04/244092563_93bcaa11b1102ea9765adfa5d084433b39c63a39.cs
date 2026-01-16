using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace CommonLib
{
    public static class SecurityCipher
    {
        private static readonly byte[] salt = Encoding.Unicode.GetBytes("salts@@");

        public static string Encrypt(string plainText, string password)
        {
            byte[] plainBytes = Encoding.Unicode.GetBytes(plainText);
            var aes = Aes.Create();

            //generating keys ,IV
            var pbkdf2 = new Rfc2898DeriveBytes(password, salt, 2000);
            var aesKey = pbkdf2.GetBytes(32);
            var aesIV = pbkdf2.GetBytes(16); 
            aes.Key = aesKey;
            aes.IV = aesIV;

            var ms = new MemoryStream();
            using (var cs = new CryptoStream(ms, aes.CreateEncryptor(), CryptoStreamMode.Write))
            {
                cs.Write(plainBytes, 0, plainBytes.Length); 
            };
            return Convert.ToBase64String(ms.ToArray()); 
        }

        public static string Decrypt(string cryptoText, string password)
        {
            byte[] cryptoBytes = Convert.FromBase64String(cryptoText.Replace(" ", "+"));
            var aes = Aes.Create();

            //generating keys ,IV
            var pbkdf2 = new Rfc2898DeriveBytes(password, salt, 2000);
            var aesKey = pbkdf2.GetBytes(32);
            var aesIV = pbkdf2.GetBytes(16);
            aes.Key = aesKey;
            aes.IV = aesIV;
            var ms = new MemoryStream();
            // var ms = new MemoryStream(cryptoBytes, 0, cryptoBytes.Length, true);
            using (var cs = new CryptoStream(ms, aes.CreateDecryptor(), CryptoStreamMode.Write))
            {
                cs.Write(cryptoBytes, 0, cryptoBytes.Length);
            };
            string DecryptedStr = Encoding.Unicode.GetString(ms.ToArray());
            ms.Close();
            return DecryptedStr;
        }
         
    }

}