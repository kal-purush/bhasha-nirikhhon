using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using System.Threading.Tasks;

namespace YourNamespace.Controllers
{
    [ApiController]
    [Route("api/movies")]
    public class MoviesController : ControllerBase
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public MoviesController(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        [HttpGet("latest")]
        public async Task<IActionResult> GetLatestMovies(String apiKey)
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

         [HttpGet("now_playing")]
        public async Task<IActionResult> GetNowPlayingMovies(String apiKey)
        {
        
            // Set the endpoint URL
            string url = $"https://api.themoviedb.org/3/movie/now_playing?api_key={apiKey}&language=en-US&page=1";

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

        [HttpGet("popular")]
        public async Task<IActionResult> GetPopularMovies(String apiKey)
        {
        
            // Set the endpoint URL
            string url = $"https://api.themoviedb.org/3/movie/popular?api_key={apiKey}&language=en-US&page=1";

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

        [HttpGet("top_rated")]
        public async Task<IActionResult> GetTopRatedMovies(String apiKey)
        {
        
            // Set the endpoint URL
            string url = $"https://api.themoviedb.org/3/movie/top_rated?api_key={apiKey}&language=en-US&page=1";

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
        [HttpGet("upcoming")]
        public async Task<IActionResult> GetUpcomingMovies(String apiKey)
        {
        
            // Set the endpoint URL
            string url = $"https://api.themoviedb.org/3/movie/upcoming?api_key={apiKey}&language=en-US&page=1";

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