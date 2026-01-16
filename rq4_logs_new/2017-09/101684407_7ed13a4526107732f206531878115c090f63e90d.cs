using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace ToyTrainProject.Models
{
    internal class AnalyticsWrapper
    {
        private readonly string apiKey;
        private readonly string endpoint;
        private readonly string mediaType;

        protected AnalyticsWrapper(string _apiKey, string _endpoint, string _mediaType)
        {
            apiKey = _apiKey;
            endpoint = _endpoint;
            mediaType = _mediaType;
        }

        protected async Task<HttpResponseMessage> callEndpoint(string apiFunction, System.Collections.Specialized.NameValueCollection queryString, byte[] byteData)
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", apiKey);

            var uri = $"{endpoint}/{apiFunction}?{queryString}";

            HttpResponseMessage response;

            using (var content = new ByteArrayContent(byteData))
            {
                content.Headers.ContentType = new MediaTypeHeaderValue(mediaType);
                response = await client.PostAsync(uri, content);
            }

            if (response.StatusCode != HttpStatusCode.OK)
            {
                throw new NotImplementedException();
            }

            return response;
        }
    }
}