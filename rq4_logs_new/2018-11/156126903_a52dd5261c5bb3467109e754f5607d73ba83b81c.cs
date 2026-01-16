using System;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace LeekWarsAPI
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            Api api = new Api();
            
            var jsonFile = File.ReadAllText("config.json");
            JObject json = JObject.Parse(jsonFile);

            string Login;
            string Pass;
            
            try
            {
                api.Url = json.Root["bot"]["url"].ToString();
                
                Login = json.Root["bot"]["login"].ToString();
                Pass = json.Root["bot"]["pass"].ToString();
            }
            catch (Exception)
            {
                throw new SystemException("LEEKWARS BOT: failed to load or parse config file");
            }

            bool isConnect = await api.Connect(Login, Pass);

            if (!isConnect)
            {
                throw new SystemException("LEEKWARS BOT: cannot connect to your account");
            }
            
            bool updatesuccessfull = await api.Update();

            await api.RegisterTounamentFarmer();
            await api.RegisterTounamentLeeks();
            
            bool disconnect = await api.Disconnect();
        }
    }
}