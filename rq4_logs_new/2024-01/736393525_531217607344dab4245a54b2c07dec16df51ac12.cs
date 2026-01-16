using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace OnlineLibriary.Tests.Utils
{

    public class APIBuilder
    {
        private readonly CustomWebApplicationFactory<API.Startup> _factory;
        public HttpClient _client;

        public APIBuilder()
        {
            _factory = new CustomWebApplicationFactory<API.Startup>();
            _client = _factory.CreateClient();
        }
        private void AssertRequestSuccessful(HttpResponseMessage response)
        {
            Assert.AreNotEqual(response.StatusCode, HttpStatusCode.InternalServerError);
            Assert.AreNotEqual(response.StatusCode, HttpStatusCode.NotFound);
        }
        public void SetAuthorizationToken(string token)
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }
        public async Task<HttpResponseMessage> GetRequest(string endPoint)
        {
            var response = await _client.GetAsync(endPoint);

            AssertRequestSuccessful(response);

            return response;
        }

        public async Task<T> GetRequestWithDeserializing<T>(string endPoint)
        {
            var response = await _client.GetAsync(endPoint);

            AssertRequestSuccessful(response);

            string res = await response.Content.ReadAsStringAsync();

            return JsonConvert.DeserializeObject<T>(res)!;
        }

        public async Task<HttpResponseMessage> PostRequest(string endPoint, object value)
        {
            var json = JsonConvert.SerializeObject(value, new JsonSerializerSettings());
            var response = await _client.PostAsync(endPoint, new StringContent(json, null, "application/json"));

            AssertRequestSuccessful(response);

            return response;
        }

        public async Task<T> PostRequestWithDeserializing<T>(string endPoint, object value) 
        {
            var json = JsonConvert.SerializeObject(value, new JsonSerializerSettings());
            var response = await _client.PostAsync(endPoint, new StringContent(json, null, "application/json"));

            AssertRequestSuccessful(response);

            string res = await response.Content.ReadAsStringAsync();

            return JsonConvert.DeserializeObject<T>(res)!;
        }

        public async Task<HttpResponseMessage> PutRequest(string endPoint, object value)
        {
            var json = JsonConvert.SerializeObject(value, new JsonSerializerSettings());
            var response = await _client.PutAsync(endPoint, new StringContent(json, null, "application/json"));

            AssertRequestSuccessful(response);

            return response;
        }

        public async Task<T> PutRequestWithDeserializing<T>(string endPoint, object value)
        {
            var json = JsonConvert.SerializeObject(value, new JsonSerializerSettings());
            var response = await _client.PutAsync(endPoint, new StringContent(json, null, "application/json"));

            AssertRequestSuccessful(response);

            string res = await response.Content.ReadAsStringAsync();

            return JsonConvert.DeserializeObject<T>(res)!;
        }

        public async Task<HttpResponseMessage> DeleteRequest(string endPoint)
        {
            var response = await _client.DeleteAsync(endPoint);

            AssertRequestSuccessful(response);

            return response;
        }

        public async Task<T> DeleteRequestWithDeserializing<T>(string endPoint)
        {
            var response = await _client.DeleteAsync(endPoint);

            AssertRequestSuccessful(response);

            string res = await response.Content.ReadAsStringAsync();

            return JsonConvert.DeserializeObject<T>(res)!;
        }
    }

}