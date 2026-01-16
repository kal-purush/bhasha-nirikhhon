using System.Text;
using System.Text.Json;
using WeatherAPI.Models;

namespace WeatherAPI
{
    public class WeatherAPIWrapper
    {
        private readonly Uri _baseAddress;
        private readonly HttpClient _client;
        private readonly string _apiKey;

        public WeatherAPIWrapper(HttpClient client, string apiKey)
        {
            _client = client;
            _baseAddress = client.BaseAddress ?? throw new Exception("Could not find base address for external API");
            _apiKey = apiKey;
            GetCityIcaoCode("Reykjavik");
        }

        private HttpContent SendRequest(UriBuilder builder)
        {
            var request = new HttpRequestMessage
            {
                RequestUri = builder.Uri,
                Method = HttpMethod.Get,
                Content = null
            };

            var response = _client.Send(request);
            response.EnsureSuccessStatusCode();
            return response.Content;
        }

        private string GetCityIcaoCode(string city)
        {
            var builder = new UriBuilder(_baseAddress)
            {
                Path = "/v3/location/search",
                Query = $"query={city}&locationType=city{GetCommonQuery(false)}"
            };
            var res = SendRequest(builder);
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var locations = res.ReadFromJsonAsync<LocationModel>(options).Result;
            var icaoCodes = locations?.Location?.IcaoCode;

            if (icaoCodes != null && icaoCodes.Length > 0)
                return icaoCodes[0];

            throw new Exception($"The location {city} was not found");
        }

        private string GetCommonQuery(bool includeUnits = true)
        {
            var query = $"&language=en-US&format=json&apiKey={_apiKey}";
            if (includeUnits)
                query += "&units";
            return query;
        }
    }
}