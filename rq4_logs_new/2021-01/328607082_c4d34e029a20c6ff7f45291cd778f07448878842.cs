using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BakalariClient.Services
{
    class HttpRequestService
    {
        /// <summary>
        /// Send request to specified url with specified cookies and body
        /// </summary>
        /// <param name="url"></param>
        /// <param name="cookieContainer"></param>
        /// <param name="body"></param>
        /// <param name="isGet"></param>
        /// <returns> Response from request </returns>
        public async Task<HttpResponseMessage> Send(string url, CookieContainer cookieContainer, object body = null, bool isGet = true)
        {
            if (body == null)
            {
                body = new { };
            }
            HttpClientHandler httpClientHandler = new HttpClientHandler()
            {
                CookieContainer = cookieContainer,
            };
            HttpClient client = new HttpClient(httpClientHandler);

            HttpContent httpContent = GetBody(body);

            HttpResponseMessage httpResponseMessage;
            if (isGet)
            {
                httpResponseMessage = await client.GetAsync(url);
            }
            else
            {
                httpResponseMessage = await client.PostAsync(url, httpContent);
            }

            if (httpResponseMessage.StatusCode != HttpStatusCode.OK)
            {
                throw new Exception("Server unavailable");
            }

            return httpResponseMessage;
        }

        /// <summary>
        /// Serialize object and convert it to HttpContent
        /// </summary>
        /// <param name="data"></param>
        /// <returns> Serialized object in HttpContent class</returns>
        private HttpContent GetBody(object data)
        {
            string myContent = JsonConvert.SerializeObject(data);
            byte[] buffer = Encoding.UTF8.GetBytes(myContent);
            return new ByteArrayContent(buffer);
        }
    }
}