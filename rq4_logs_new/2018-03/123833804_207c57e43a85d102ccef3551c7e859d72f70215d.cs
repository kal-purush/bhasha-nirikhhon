using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography;

namespace FileEncryptionTool
{
    class User : IEquatable<User>
    {
        public string Email { get; }
        private string _privateKeyPath;
        private string _publicKeyPath;

        private static string _programDataDir =
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "FileEncryptionTool");

        private static string _publicKeysDir = Path.Combine(_programDataDir, "public");
        private static string _privateKeysDir = Path.Combine(_programDataDir, "private");


        public User(string email, string password)
        {
            this.Email = email;
            generateKeyPair(email, password);
        }

        private User(string email, string privateKeyPath, string publicKeyPath)
        {
            this.Email = email;
            this._privateKeyPath = privateKeyPath;
            this._publicKeyPath = publicKeyPath;
        }


        public static List<User> loadUsers()
        {
            List<User> allUsers = new List<User>();

            if (!Directory.Exists(_programDataDir))
            {
                return allUsers;
            }

            string[] keyPaths = Directory.GetFiles(_publicKeysDir, "*");
            foreach (string publicKeyPath in keyPaths)
            {
                string email = Path.GetFileNameWithoutExtension(publicKeyPath);
                string privateKeyPath = Path.Combine(_privateKeysDir, email);
                allUsers.Add(new User(email, privateKeyPath, publicKeyPath));
            }

            return allUsers;
        }

        private static void createDirectory(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
        }

        private void generateKeyPair(string email, string password)
        {
            createDirectory(_publicKeysDir);
            createDirectory(_privateKeysDir);


            this._publicKeyPath = Path.Combine(_publicKeysDir, email);
            this._privateKeyPath = Path.Combine(_privateKeysDir, email);



            using (var rsa = new RSACryptoServiceProvider(1024))
            {
                try
                {
                    string publicOnlyKeyXML = rsa.ToXmlString(false);

                    
                    using(StreamWriter fs = new StreamWriter(this._publicKeyPath))
                    {
                        fs.Write(publicOnlyKeyXML);
                    }

                    //TODO: add private key encryption
                    string privateKeyXML = rsa.ToXmlString(true);

                    using (StreamWriter fs = new StreamWriter(this._privateKeyPath))
                    {
                        fs.Write(privateKeyXML);
                    }



                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    rsa.PersistKeyInCsp = false;
                }
            }


        }

        public string getPublicKey()
        {
            using(StreamReader fs = new StreamReader(this._publicKeyPath))
            {
                return fs.ReadToEnd();
            }
        }
        

        public bool Equals(User other)
        {
            return other.Email == this.Email;
        }

        public override string ToString()
        {
            return this.Email;
        }
    }
}