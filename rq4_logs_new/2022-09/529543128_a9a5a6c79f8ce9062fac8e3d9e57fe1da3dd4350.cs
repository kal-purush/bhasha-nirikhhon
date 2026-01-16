using Home.Common;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Home.Graph.Common
{
    public static class LocalDebugHelper
    {
        private static string _serviceHost = null;
        private static string _graphHost = null;
        private static string _apiKey = null;

        static LocalDebugHelper()
        {
            if (File.Exists("local.debug.json"))
                ReadFile("local.debug.json");
            else if (File.Exists("../../../local.debug.json"))
                ReadFile("../../../local.debug.json");
            else
            {
                var pth = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "manoir.app", "local.debug.json");
                if (File.Exists(pth))
                    ReadFile(pth);
            }
        }

        private class Config
        {
            public string ServiceHost { get; set; }
            public string GraphHost { get; set; }
            public string ApiKey { get; set; }
        }
        private static void ReadFile(string v)
        {
            var tmp = File.ReadAllText(v);
            var tmpObj = JsonConvert.DeserializeObject<Config>(tmp);
            _serviceHost = tmpObj?.ServiceHost;
            _graphHost = tmpObj?.GraphHost;
            _apiKey = tmpObj?.ApiKey;
        }

        public static string GetLocalServiceHost()
        {
            if (_serviceHost == null)
                return HomeServerHelper.GetLocalIP();
            return _serviceHost;
        }

        public static string GetLocalGraphHost()
        {
            if (_graphHost == null)
                return HomeServerHelper.GetLocalIP();
            return _graphHost;
        }

        public static string GetApiKey()
        {
            if (_apiKey == null)
                return Environment.GetEnvironmentVariable("HOMEAUTOMATION_APIKEY");

            return _apiKey;
        }
    }
}