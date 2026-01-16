using System.Net.Http;
using System.Threading.Tasks;

namespace FootballClient.DataAccess
{
    public interface IRestClient
    {
        Task<T> SendMessageAsync<T>(HttpRequestMessage requestMessage, IParserStrategy<T> parser);
    }

    public class RestClient : IRestClient
    {
        private HttpClient _httpClient;
        private HttpClientHandler _httpClientHandler;
        public RestClient()
        {
            _httpClientHandler = new HttpClientHandler()
            {
                AutomaticDecompression = System.Net.DecompressionMethods.Deflate | System.Net.DecompressionMethods.GZip
            };
            _httpClient = new HttpClient(_httpClientHandler);
        }

        public async Task<T> SendMessageAsync<T>(HttpRequestMessage requestMessage, IParserStrategy<T> parser)
        {
            using (var httpResult = await _httpClient.SendAsync(requestMessage))
            {
                var data = await httpResult.Content.ReadAsStringAsync();
                var parsedData = parser.Parse(data);
                return parsedData;
            }
        }
    }
}