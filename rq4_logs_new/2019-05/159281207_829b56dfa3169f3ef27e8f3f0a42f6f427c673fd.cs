using DataAccessLayer.Logging;
using ServiceLayer.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BroadwayBuilder.Api
{
    public static class LoggerHelper
    {
        public static void LogUsage(string product, int userId)
        {
            var httpRequest = HttpContext.Current.Request;
            var IpAddress = httpRequest.UserHostAddress; //get IP address of the user that sent the request
            string url, httpMethod;
            var data = GetRequestData(httpRequest, out url, out httpMethod);
            var usageLog = new UsageLog() {
            HttpMethod = httpMethod,
            Request = url,
            Product = product,
            UserId = userId,
            AdditionalInfo = data};
            try
            {
                var usageLogService = new UsageLogService();
                usageLogService.CreateUsageLog(usageLog);
            }
            catch
            {
                //TODO: Try log and update failed # of tries 
            }
        }

        private static Dictionary<string, object> GetRequestData(HttpRequest httpRequest,out string url, out string httpMethod)
        {
            var data = new Dictionary<string, object>();
            url = "";
            httpMethod = "";
            if (httpRequest != null)
            {
                url = httpRequest.Url.ToString();
                httpMethod = httpRequest.HttpMethod;
                //IpAddress = httpRequest.UserHostAddress;
                string browserType, browserVersion;
                GetBrowserInfo(httpRequest, out browserType, out browserVersion);
                data.Add("Browser", $"{browserType}{browserVersion}");
                GetRequestParameters(httpRequest, data);
            }

            return data;

        }

        private static void GetBrowserInfo(HttpRequest request, out string type, out string version)
        {
            type = request.Browser.Type;
            version = " (v " + request.Browser.MajorVersion + "." + request.Browser.MinorVersion + ")";
        }

        private static void GetRequestParameters(HttpRequest httpRequest, Dictionary<string, object> data)
        {
            var qs = httpRequest.QueryString;
            var i = 0;
            foreach (string key in qs.Keys)
            {
                var newKey = string.Format("Parameter-{0}-{1}", i++, key);
                if (!data.ContainsKey(newKey))
                {
                    data.Add(newKey, qs[key]);
                }
            }
        }
    }
}