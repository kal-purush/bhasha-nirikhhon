using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Nancy;
using Newtonsoft.Json;

namespace DistanceService
{
    public static class ResponseExtensions
    {
        public static Response CreateResponse(this HttpStatusCode statusCode, string statusText) 
            => statusCode.CreateResponse(statusText, "application/json");

        public static async Task<Response> CreateResponse(this HttpResponseMessage httpResponseMessage)
        {
            var statusText = await httpResponseMessage.Content.ReadAsStringAsync();
            return ((HttpStatusCode)httpResponseMessage.StatusCode)
                .CreateResponse(statusText, httpResponseMessage.Content.Headers.ContentType.ToString(), httpResponseMessage.ReasonPhrase);
        }

        public static Response CreateResponse(this HttpStatusCode statusCode, string statusText, string contentType, string reasonPhrase = "")
        {
            if (string.IsNullOrEmpty(statusText))
                throw new ArgumentNullException(nameof(statusText));

            var jsonBytes = Encoding.UTF8.GetBytes(statusText);
            return new Response
            {
                ContentType = contentType,
                Contents = s => s.Write(jsonBytes, 0, jsonBytes.Length),
                StatusCode = statusCode,
                ReasonPhrase = reasonPhrase
            };
        }

        public static T GetModel<T>(this Response response)
        {
            var stringContent = string.Empty;
            using (var ms = new MemoryStream())
            {
                response.Contents(ms);
                ms.Flush();
                ms.Position = 0;

                stringContent = Encoding.UTF8.GetString(ms.ToArray());
            }

            return JsonConvert.DeserializeObject<T>(stringContent);
        }
    }
}