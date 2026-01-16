using Newtonsoft.Json;
using OpenWeatherMapTest.Models;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace OpenWeatherMap.Services
{
    class OpenWeatherMapService
    {
        private readonly Uri Uri = new Uri("http://api.openweathermap.org/data/2.5/");

        private readonly string ApiKey = "APPID=4ea98f74622072acddf2f67deb259125";

        private async Task<T> GetAsync<T>(Uri uri)
        {
            using (var client = new HttpClient())
            {
                // Adding the required headers
                client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                // Ignoring null values and missing members from response
                var settings = new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore
                };

                // Getting the response
                var response = await client.GetAsync(uri);

                // Reading response as string
                var json = await response.Content.ReadAsStringAsync();

                // Deserializing to the appropriate object
                T result = JsonConvert.DeserializeObject<T>(json, settings);
                return result;
            }
        }

        public async Task<WeatherData> GetWeatherForCityAsync(string query) => await GetAsync<WeatherData>(new Uri(Uri, $"weather?q={query}&{ApiKey}"));
    }
}