using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BakalariClient.Models;
using Newtonsoft.Json;

namespace BakalariClient.Services
{
    class CredentialService
    {
        private readonly string filename;
        private Config config;
        public CredentialService(string filename = "config.json")
        {
            //filename = @"..\..\" + filename;
            this.filename = filename;
        }

        /// <summary>
        /// Read file and return contents
        /// </summary>
        /// <returns></returns>
        public Config GetCredentials()
        {
            if (config == null)
            {
                return JsonConvert.DeserializeObject<Config>(File.ReadAllText(this.filename));
            }

            return config;
        }

        public Config SetCredentials(Config config)
        {
            string configText = JsonConvert.SerializeObject(config);
            File.WriteAllText(this.filename, configText);
            return config;
        }
    }
}