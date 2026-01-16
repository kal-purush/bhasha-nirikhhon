using Newtonsoft.Json.Linq;
using System;
using System.Net;

namespace Blizzard.SC2
{
    public class ApiHelper
    {
        public static dynamic GetJsonFromUrl(string url)
        {
            try
            {
                return JObject.Parse(new WebClient().DownloadString(url));
            }
            catch { return null; }
        }

        public static object CreateNewInstance(Type type, object[] args)
        {
            return Activator.CreateInstance(type, args);
        }
    }
}