using System.Text;
using Homework17.Models;
using Newtonsoft.Json;

namespace Homework17.HttpClients
{
    public class BasicHttpClient
    {
        private static HttpClient _httpClient;
        private static HttpRequestMessage _httpRequestMessage;
        private static RestResponse _restResponse;

        public static RestResponse PerformGetRequest(string requestUrl, Dictionary<string, string> headers) =>
            SendRequest(requestUrl, HttpMethod.Get, null, headers);

        public static RestResponse PerformPostRequest(string requestUrl, HttpContent httpContent, Dictionary<string, string> headers) =>
            SendRequest(requestUrl, HttpMethod.Post, httpContent, headers);

        public static RestResponse<T> PerformGetRequest<T>(string requestUrl, Dictionary<string, string> headers) where T : class=>
            SendRequest<T>(requestUrl, HttpMethod.Get, null, headers);

        public static RestResponse<T> PerformPostRequest<T>(string requestUrl, HttpContent httpContent, Dictionary<string, string> headers) where T : class =>
            SendRequest<T>(requestUrl, HttpMethod.Post, httpContent, headers);

        public static RestResponse<TResponse> PerformPostRequest<TRequest, TResponse>(
            string requestUrl,
            TRequest requestModel,
            Dictionary<string, string> headers)
            where TRequest : class
            where TResponse : class =>
            SendRequest<TRequest, TResponse>(requestUrl, HttpMethod.Post, requestModel, headers);

        public static RestResponse<TResponse> PerformPutRequest<TRequest, TResponse>(
            string requestUrl,
            TRequest requestModel,
            Dictionary<string, string> headers)
            where TRequest : class
            where TResponse : class =>
            SendRequest<TRequest, TResponse>(requestUrl, HttpMethod.Put, requestModel, headers);

        public static RestResponse PerformDeleteRequest(string requestUrl, Dictionary<string, string> headers) =>
            SendRequest(requestUrl, HttpMethod.Delete, null, headers);

        private static HttpClient CreateHttpClientAndAddHeaders(Dictionary<string, string> headers)
        {
            var httpClient = new HttpClient();

            if (headers != null)
            {
                foreach (var header in headers)
                {
                    httpClient.DefaultRequestHeaders.Add(header.Key, header.Value);
                }
            }

            return httpClient;
        }

        private static HttpRequestMessage CreateHttpRequestMessage(string requestUrl, HttpMethod httpMethod,
            HttpContent httpContent)
        {
            HttpRequestMessage httpRequestMessage = new HttpRequestMessage();
            httpRequestMessage.Method = httpMethod;
            httpRequestMessage.RequestUri = new Uri(requestUrl);

            if (httpMethod != HttpMethod.Get)
            {
                httpRequestMessage.Content = httpContent;
            }

            return httpRequestMessage;
        }

        private static RestResponse SendRequest(string requestUrl, HttpMethod httpMethod, HttpContent httpContent, Dictionary<string, string> headers)
        {
            _httpClient = CreateHttpClientAndAddHeaders(headers);
            _httpRequestMessage = CreateHttpRequestMessage(requestUrl, httpMethod, httpContent);
            var httpResponse = _httpClient.SendAsync(_httpRequestMessage).Result;

            try
            {
                _restResponse = new RestResponse(httpResponse);
            }
            catch (Exception e)
            {
                throw new Exception($"Error with response: Status code: {httpResponse.StatusCode}, \nException: {e.Message}");
            }
            finally
            {
                _httpRequestMessage?.Dispose();
                _httpClient?.Dispose();
            }

            return _restResponse;
        }

        private static RestResponse<TResponse> SendRequest<TRequest, TResponse>(
            string requestUrl,
            HttpMethod httpMethod,
            TRequest requestModel,
            Dictionary<string, string> headers)
            where TRequest : class
            where TResponse : class
        {
            _httpClient = CreateHttpClientAndAddHeaders(headers);
            var httpContentString = JsonConvert.SerializeObject(requestModel);
            var httpContent = new StringContent(httpContentString, Encoding.UTF8, "application/json");
            _httpRequestMessage = CreateHttpRequestMessage(requestUrl, httpMethod, httpContent);
            var httpResponse = _httpClient.SendAsync(_httpRequestMessage).Result;
            RestResponse<TResponse> restResponse;

            try
            {
                restResponse = new RestResponse<TResponse>(httpResponse);
            }
            catch (Exception e)
            {
                throw new Exception($"Error with response: Status code: {httpResponse.StatusCode}, \nException: {e.Message}");
            }
            finally
            {
                _httpRequestMessage?.Dispose();
                _httpClient?.Dispose();
            }

            return restResponse;
        }

        private static RestResponse<T> SendRequest<T>(string requestUrl, HttpMethod httpMethod, HttpContent httpContent, Dictionary<string, string> headers) where T : class
        {
            _httpClient = CreateHttpClientAndAddHeaders(headers);
            _httpRequestMessage = CreateHttpRequestMessage(requestUrl, httpMethod, httpContent);
            var httpResponse = _httpClient.SendAsync(_httpRequestMessage).Result;

            try
            {
                _restResponse = new RestResponse<T>(httpResponse);
            }
            catch (Exception e)
            {
                throw new Exception($"Error with response: Status code: {httpResponse.StatusCode}, \nException: {e.Message}");
            }
            finally
            {
                _httpRequestMessage?.Dispose();
                _httpClient?.Dispose();
            }

            return _restResponse as RestResponse<T>;
        }

    }
}