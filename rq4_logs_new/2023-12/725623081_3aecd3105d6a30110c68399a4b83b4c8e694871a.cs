using System.Net.Http.Json;
using Shared;

namespace FrontendBlazorWebAssembly.Services;

public class MovieService : IMovieService
{
    private readonly HttpClient _httpClient;
    //const string _baseUrl = "https://localhost:7254";
    // public MoviesService(HttpClient httpClient) => _httpClient = httpClient;

    public MovieService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }
    public  async Task<List<Movie>> GetAllMoviesByTitle(string title)
    {
        var moviesByTitle = await _httpClient.GetFromJsonAsync<List<Movie>>($"/Movie/title/{title}");
        return moviesByTitle;
    }

    public async Task<List<Movie>> GetAllMovies()
    {
        var allMovies = await _httpClient.GetFromJsonAsync<List<Movie>>("/Movie");
        return allMovies;
    }
}