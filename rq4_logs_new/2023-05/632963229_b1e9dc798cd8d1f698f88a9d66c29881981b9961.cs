using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using System.Net.Http;
using System.Text.Json;
using MovieFiles.Core.Models;

namespace MovieFiles.Api.Functions
{
    public class Movies
    {
        private readonly ILogger<Movies> _logger;
        private readonly string _apiKey;

        public Movies(ILogger<Movies> log)
        {
            _logger = log;
            _apiKey = System.Environment.GetEnvironmentVariable("MOVIE_API_KEY");
        }

        [FunctionName("GetPopularMovies")]
        [OpenApiOperation(operationId: "GetPopularMovies", tags: new[] { "Movies" })]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(MovieList), Description = "The OK response")]
        public async Task<IActionResult> GetPopularMovies(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "movies/popular")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string apiKey = System.Environment.GetEnvironmentVariable("MOVIE_API_KEY");
            using HttpClient client = new HttpClient(); 
            using HttpResponseMessage response = await client.GetAsync($"https://api.themoviedb.org/3/movie/popular?api_key={apiKey}");
            response.EnsureSuccessStatusCode();
            
            string responseMessage = await response.Content.ReadAsStringAsync();
            
            MovieList list = JsonSerializer.Deserialize<MovieList>(responseMessage);

            return new OkObjectResult(list);
        }
    }
}
