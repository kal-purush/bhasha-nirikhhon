using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Utilities
{
    public class RSAFileUserLoader : IUserLoader
    {
        private string usersFolderPath = AppDomain.CurrentDomain.BaseDirectory + "UsersData\\";
        private string key;
        private RSAParameters rsaParameters;

        public RSAFileUserLoader()
        {
            Init();
        }

        public void Init()
        {
            var keysPath = usersFolderPath + "keys.txt";
            try
            {
                if (File.Exists(keysPath))
                {
                    using (var reader = new StreamReader(keysPath))
                    {
                        key = reader.ReadLine();
                    }
                }
                else
                {
                    using (RSA rsa = RSA.Create())
                    {
                        rsaParameters = rsa.ExportParameters(true);
                        using (var writer = new StreamWriter(keysPath))
                        {
                            key = rsa.ToXmlString(false);
                            writer.WriteLine(key);                            
                        }
                    }
                }
            }
            catch
            {

            }
        }

        public User Load(string name)
        {
            var fullPath = usersFolderPath + name + ".txt";
            try
            {
                string textFromFile;
                User user;
                using (var reader = new StreamReader(fullPath))
                {
                    textFromFile = reader.ReadLine();
                }
                byte[] buffer = Encoding.UTF8.GetBytes(textFromFile);
                using (RSA rsa = RSA.Create())
                {
                    rsa.FromXmlString(key);
                    var decryptedData = RSADecrypt(buffer, rsaParameters);
                    
                    user = new User(name, BitConverter.ToInt32(decryptedData, 0));
                }
                return user;
            }
            catch (Exception ex)
            {
                // Молчаливое сглатывание
            }
            return null;
        }

        public void Save(User user)
        {
            var fullPath = usersFolderPath + user.Name + ".txt";
            try
            {
                byte[] buffer = Encoding.UTF8.GetBytes(user.Money.ToString());

                using (RSA rsa = RSA.Create())
                {
                    rsa.FromXmlString(key);
                    var encryptedData = RSAEncrypt(buffer, rsaParameters);
                    using (StreamWriter writer = new StreamWriter(fullPath))
                    {
                        writer.WriteLine(BitConverter.ToString(encryptedData, 0));
                    }
                }              
            }
            catch (Exception ex)
            {
                // Молчаливое сглатывание
            }
        }


        private byte[] RSAEncrypt(byte[] DataToEncrypt, RSAParameters RSAKeyInfo)
        {
            try
            {
                byte[] encryptedData;
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider(2048))
                {
                    RSA.ImportParameters(RSAKeyInfo);
                    RSA.FromXmlString(key);
                    encryptedData = RSA.Encrypt(DataToEncrypt, false);
                }
                return encryptedData;
            }

            catch (CryptographicException e)
            {
                return null;
            }
        }

        private byte[] RSADecrypt(byte[] DataToDecrypt, RSAParameters RSAKeyInfo)
        {
            try
            {
                byte[] decryptedData;
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider(2048))
                {
                    RSA.FromXmlString(key);
                    RSA.ImportParameters(RSAKeyInfo);
                    decryptedData = RSA.Decrypt(DataToDecrypt, false);
                }
                return decryptedData;
            }
            catch (CryptographicException e)
            {
                return null;
            }

        }
    }
}
