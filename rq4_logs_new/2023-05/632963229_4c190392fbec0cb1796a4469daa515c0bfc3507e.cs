using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using System.Threading.Tasks;

namespace YourNamespace.Controllers
{
    [ApiController]
    [Route("api/movie")]
    public class MovieDetailsService : ControllerBase
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public MovieDetailsService(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        [HttpGet("details")]
        public async Task<IActionResult> GetMovieDetails(String apiKey, String movieId)
        {
        
            // Set the endpoint URL
            string url = $"https://api.themoviedb.org/3/movie/latest?api_key={apiKey}&language=en-US&page=1";

            HttpClient client = _httpClientFactory.CreateClient();
            try
            {
                HttpResponseMessage response = await client.GetAsync(url);
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();

                return Ok(responseBody);
            }
            catch (HttpRequestException ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }
    }
}