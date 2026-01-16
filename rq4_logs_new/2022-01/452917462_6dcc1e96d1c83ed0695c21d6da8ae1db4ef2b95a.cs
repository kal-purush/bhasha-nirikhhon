using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Net.Http;
using System.IO;

namespace LuminoSaber.Hue
{
    public class CreateUser
    {
        public static string Usercode;

        private const string APIAddressTemplate = "http://{0}/api";
        private const string BodyTemplate = "{{\"devicetype\":\"{0}\"}}";

        public static string ConnectBridge(string username, string BridgeIP)
        {
            return PostRequestToBridge(
                string.Format(APIAddressTemplate, BridgeIP),
                string.Format(BodyTemplate, username));
        }

        private static string PostRequestToBridge(string uri, string data, string contentType = "application/json", string method = "POST")
        {
            byte[] dataBytes = Encoding.UTF8.GetBytes(data);
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
            request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
            request.ContentLength = dataBytes.Length;
            request.ContentType = contentType;
            request.Method = method;

            using (Stream requestBody = request.GetRequestStream())
            {
                requestBody.Write(dataBytes, 0, dataBytes.Length);
            }

            // Search for an easier way
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            using (Stream stream = response.GetResponseStream())
            using (StreamReader reader = new StreamReader(stream))
            {
                string json = reader.ReadToEnd();

                var result = ResultHelper.FromJson(json);
                Usercode = result.First().Success.Username;

                return json;
            }
        }
    }
}