using System;
using System.IO;
using System.Net.Mime;
using System.Text;

namespace Ridavei.FileCryption.Tests.Extensions
{
    internal static class DecryptExt
    {
        public static AFileCryptionBuilderBase UseDecryptTxtExt(this FileDecryptionBuilder builder)
        {
            return builder.AddDecryptionMethod(new ContentType(MediaTypeNames.Text.Plain), DecryptTxt);
        }

        private static Stream DecryptTxt(Stream file, string password)
        {
            string decryptedString = DecryptString(TestHelper.ReadTextFromFile(file), password);

            MemoryStream ms = new MemoryStream();
            var sw = new StreamWriter(ms);
            sw.Write(decryptedString);
            sw.Flush();
            ms.Position = 0;
            return ms;
        }

        private static string DecryptString(string stringToDecrypt, string key)
        {
            System.Security.Cryptography.CspParameters cspp = new System.Security.Cryptography.CspParameters
            {
                KeyContainerName = key
            };

            System.Security.Cryptography.RSACryptoServiceProvider rsa = new System.Security.Cryptography.RSACryptoServiceProvider(cspp)
            {
                PersistKeyInCsp = true
            };

            string[] decryptArray = stringToDecrypt.Split(new string[] { "-" }, StringSplitOptions.None);
            byte[] decryptByteArray = Array.ConvertAll<string, byte>(decryptArray, (s => Convert.ToByte(byte.Parse(s, System.Globalization.NumberStyles.HexNumber))));

            byte[] bytes = rsa.Decrypt(decryptByteArray, true);

            return UTF8Encoding.UTF8.GetString(bytes);
        }
    }
}