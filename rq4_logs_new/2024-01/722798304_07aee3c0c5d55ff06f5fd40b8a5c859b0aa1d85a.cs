using CASHelpers;
using CASHelpers.Types.HttpResponses.BenchmarkAPI;
using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;

namespace CasDotnetSdk.Http
{
    internal class BenchmarkSender
    {
        public async Task SendNewBenchmarkMethod(string methodName, DateTime start, DateTime end, BenchmarkMethodType type, string? methodDescription = null)
        {
            HttpClientSingleton httpClient = HttpClientSingleton.Instance;
            string url = CASConfiguration.Url + Constants.ApiRoutes.MethodBenchmark;
            BenchmarkSDKMethod newBenchmark = new BenchmarkSDKMethod()
            {
                MethodDescription = methodDescription,
                MethodStart = start,
                MethodEnd = end,
                MethodName = methodName,
                MethodType = type,
            };
            string stringBenchmark = JsonSerializer.Serialize(newBenchmark);
            HttpResponseMessage response = await httpClient.PostAsJsonAsync(url, stringBenchmark);
            if (!response.IsSuccessStatusCode)
            {
                // Put into retry queue
                CASConfiguration.BenchmarkSenderQueue.Enqueue(newBenchmark);
            }
        }

        public async Task<bool> SendNewBenchmarkMethodRetry(BenchmarkSDKMethod retryBenchmark)
        {
            HttpClientSingleton httpClient = HttpClientSingleton.Instance;
            string url = CASConfiguration.Url + Constants.ApiRoutes.MethodBenchmark;
            string stringBenchmark = JsonSerializer.Serialize(retryBenchmark);
            HttpResponseMessage response = await httpClient.PostAsJsonAsync(url, stringBenchmark);
            return response.IsSuccessStatusCode;
        }
    }
}