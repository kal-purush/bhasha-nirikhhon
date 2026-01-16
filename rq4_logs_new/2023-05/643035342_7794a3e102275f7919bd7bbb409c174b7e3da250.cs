using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace WeatherAPI.Controllers
{

    [Route("[controller]")]
    [ApiController]
    public class WeatherController : ControllerBase
    {
        private readonly HttpClient _client;
        private readonly RequestBuilder _requests;

        public WeatherController(IHttpClientFactory clientFactory)
        {
            _client = clientFactory.CreateClient("Weather.com");
            if (_client.BaseAddress == null)
                throw new Exception("Could not find base address for external API");

            _requests = new RequestBuilder(_client.BaseAddress);
        }

        [HttpGet(Name = "GetWeather")]
        public string Get()
        {
            
            var response = _client.Send(request);
            response.EnsureSuccessStatusCode();
            return response.Content.ToString();
        }
    }
}