using install.Exceptions;
using install.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace install.Hash
{
    class HashWorker : HashWorkerInterface<HashConfig>
    {
        HashConfig config;

        public string getHash(string password, string sult)
        {
            if (config != null)
            {
                for (int i = 0; i < config.numberOfHashing; i++)
                {
                    password = getHash(anythingPlusSult(password, sult));
                }

                return password;
            }
            else
            {
                throw new NoConfigurationSpecified("No configuration specified");
            }
        }

        private string anythingPlusSult(string anything, string sult)
        {
            return anything + sult;
        }

        public string getSult(SecurityUserInterface user)
        {
            string sult = "";
            Random rand = new Random();
            for (int i = 0; i < config.sultLength; i++)
            {
                int wordInInt = 91;
                while (wordInInt > 90 & wordInInt < 97)
                {
                    wordInInt = rand.Next(65, 122);
                }
                char c = (char)wordInInt;
                sult += c.ToString();
            }

            return sult;
        }

        public void setConfig(HashConfig config)
        {
            this.config = config;
        }

        private string getHash(string password)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(password);
            SHA256Managed hashstring = new SHA256Managed();
            byte[] hash = hashstring.ComputeHash(bytes);
            string hashString = string.Empty;
            foreach (byte x in hash)
            {
                hashString += String.Format("{0:x2}", x);
            }
            return hashString;
        }
    }
}