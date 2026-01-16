using System;
using System.Net;
using Newtonsoft.Json;

namespace qfe
{
    class API
    {
        private static String siteUrl = "http://ivao.daitel.net/api/qfe/";

        public static String lastError = "";

        public static APIResponse getWx(String icao)
        {
            String serverResponse = API.request(icao);
            return JsonConvert.DeserializeObject<APIResponse>(serverResponse);
        }

        private static string request(String request)
        {
            String url = API.siteUrl + request;
 
            using (WebClient client = new WebClient())
            {
                return client.DownloadString(url);
            }
        }
    }
}