using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;

namespace Utilities
{
    public class CryptoFileUserLoader : IUserLoader
    {
        private string usersFolderPath = AppDomain.CurrentDomain.BaseDirectory + "UsersData\\";
        private byte[] key;
        private byte[] iv;

        public CryptoFileUserLoader()
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
                        key = Encoding.UTF8.GetBytes(reader.ReadLine());
                        iv = Encoding.UTF8.GetBytes(reader.ReadLine());
                    }
                }
                else
                {
                    using (Rijndael rijndael = Rijndael.Create())
                    {
                        using (var writer = new StreamWriter(keysPath))
                        {
                            key = rijndael.Key;
                            iv = rijndael.IV;
                            writer.WriteLine(Encoding.UTF8.GetString(key));
                            writer.WriteLine(Encoding.UTF8.GetString(iv));
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
                using (RSA rijAlg = RSA.Create())
                {
                    rijAlg.Key = key;
                    rijAlg.IV = iv;
                    ICryptoTransform decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV);

                    using (MemoryStream msDecrypt = new MemoryStream(buffer))
                    {
                        using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                        {
                            using (StreamReader srDecrypt = new StreamReader(csDecrypt))
                            {
                                var plaintext = srDecrypt.ReadToEnd();
                                user = new User(name, int.Parse(plaintext));
                            }
                        }
                    }
                }
                return user;
            }
            catch(Exception ex)
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
                byte[] encrypted;
                using (Rijndael rijAlg = Rijndael.Create())
                {
                    rijAlg.Key = key;
                    rijAlg.IV = iv;
                    ICryptoTransform encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV);

                    using (MemoryStream msEncrypt = new MemoryStream())
                    {
                        using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                        {
                            using (StreamWriter swEncrypt = new StreamWriter(csEncrypt))
                            {
                                swEncrypt.Write(user.Money.ToString());
                                csEncrypt.FlushFinalBlock();
                            }
                            encrypted = msEncrypt.ToArray();
                        }
                    }
                }
                using (var writer = new StreamWriter(fullPath))
                {
                    writer.WriteLine(Encoding.UTF8.GetString(encrypted, 0, encrypted.Length));
                    
                }
            }
            catch(Exception ex)
            {
                // Молчаливое сглатывание
            }
        }


        public static byte[] RSAEncrypt(byte[] DataToEncrypt, RSAParameters RSAKeyInfo)
        {
            try
            {
                byte[] encryptedData;
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider())
                {
                    RSA.ImportParameters(RSAKeyInfo);
                    
                    encryptedData = RSA.Encrypt(DataToEncrypt, false);
                }
                return encryptedData;
            }

            catch (CryptographicException e)
            {
                return null;
            }

        }

        public static byte[] RSADecrypt(byte[] DataToDecrypt, RSAParameters RSAKeyInfo)
        {
            try
            {
                byte[] decryptedData;
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider())
                {
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