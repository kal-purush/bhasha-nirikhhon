using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;

namespace Calculator.Integration
{
    public class MultiplyAddIntegrationTest
    {
        public const string CalculatorApiRoot = "http://localhost:8080/api/calculator/";

        [Theory]
        [InlineData(0, 0)]
        [InlineData(0, 1)]
        [InlineData(1, 0)]
        [InlineData(1, 1)]
        [InlineData(2, 0)]
        [InlineData(2, 1)]
        [InlineData(2, 2)]
        public async Task Multiply(int a, int b)
        {
            var client = new HttpClient { BaseAddress = new Uri(CalculatorApiRoot) };
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            Console.WriteLine($"Sending multiply request: {nameof(a)}:{a} and {nameof(b)}{b}");

            var request = await client.GetAsync($"multiply/{a}/{b}");
            request.EnsureSuccessStatusCode();
            var content = await request.Content.ReadAsStringAsync();
            var result = JsonConvert.DeserializeObject<int>(content);
            var expected = a * b;

            Assert.Equal(expected, result);
        }
    }
}