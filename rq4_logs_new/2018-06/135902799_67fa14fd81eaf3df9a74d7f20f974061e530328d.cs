using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Security.Cryptography;

namespace CSharpSecurityInformationSystem
{
    class CryptoClass
    {
        private static string algor = "Fy5lfsRlh6bBXq3m";
        private static string key = "e4aFy5lfHrsRlhw6bBmdXqd33mlCtO8E";

        public string Encrypt(string Decryptd)
        {
            byte[] byteEn = ASCIIEncoding.ASCII.GetBytes(Decryptd);
            AesCryptoServiceProvider endcod = new AesCryptoServiceProvider();
            endcod.BlockSize = 128;
            endcod.KeySize = 256;
            endcod.Key = ASCIIEncoding.ASCII.GetBytes(key);
            endcod.IV = ASCIIEncoding.ASCII.GetBytes(algor);
            endcod.Padding = PaddingMode.PKCS7;
            endcod.Mode = CipherMode.CBC;

            ICryptoTransform trans = endcod.CreateEncryptor(endcod.Key, endcod.IV);
            byte[] enc = trans.TransformFinalBlock(byteEn, 0, byteEn.Length);
            trans.Dispose();
            return Convert.ToBase64String(enc);

        }


    }
}