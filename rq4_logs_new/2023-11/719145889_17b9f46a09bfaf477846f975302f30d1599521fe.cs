using System.Security.Cryptography;
using System.Text;

namespace legallead.jdbc
{
    public static class CryptoManager
    {
        public static string Encrypt(string input, string key, out string vector)
        {
            byte[] inputArray = Encoding.UTF8.GetBytes(key);
            var key64 = Convert.ToBase64String(inputArray, 0, inputArray.Length);
            return EncryptData(input, key64, out vector);
        }

        public static string Decrypt(string input, string key, string vectorBase64)
        {
            byte[] inputArray = Encoding.UTF8.GetBytes(key);
            var key64 = Convert.ToBase64String(inputArray, 0, inputArray.Length);
            return DecryptData(input, key64, vectorBase64);
        }

        private static string EncryptData(string plainText, string keyBase64, out string vectorBase64)
        {
            using Aes aesAlgorithm = Aes.Create();
            aesAlgorithm.Key = Convert.FromBase64String(keyBase64);
            aesAlgorithm.GenerateIV();

            vectorBase64 = Convert.ToBase64String(aesAlgorithm.IV);
            // Create encryptor object
            ICryptoTransform encryptor = aesAlgorithm.CreateEncryptor();

            byte[] encryptedData;

            //Encryption will be done in a memory stream through a CryptoStream object
            using (MemoryStream ms = new())
            {
                using CryptoStream cs = new(ms, encryptor, CryptoStreamMode.Write);
                using (StreamWriter sw = new(cs))
                {
                    sw.Write(plainText);
                }
                encryptedData = ms.ToArray();
            }

            return Convert.ToBase64String(encryptedData);
        }

        private static string DecryptData(string cipherText, string keyBase64, string vectorBase64)
        {
            using Aes aesAlgorithm = Aes.Create();
            aesAlgorithm.Key = Convert.FromBase64String(keyBase64);
            aesAlgorithm.IV = Convert.FromBase64String(vectorBase64);

            // Create decryptor object
            ICryptoTransform decryptor = aesAlgorithm.CreateDecryptor();

            byte[] cipher = Convert.FromBase64String(cipherText);

            //Decryption will be done in a memory stream through a CryptoStream object
            using MemoryStream ms = new(cipher);
            using CryptoStream cs = new(ms, decryptor, CryptoStreamMode.Read);
            using StreamReader sr = new(cs);
            return sr.ReadToEnd();
        }
    }
}