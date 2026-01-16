using MovieFiles.Api.Client.Mappers;
using MovieFiles.Api.Client.Models;

namespace MovieFiles.Api.Client.Services;

public class MoviesService: IMoviesService
{
    private readonly MovieFilesFunctions _client;
    private readonly string _functionAppKey;
    public MoviesService(string httpUrl, string functionAppKey)
    {
        _client = new MovieFilesFunctions(new HttpClient { BaseAddress = new Uri(httpUrl) });
        _client.BaseUrl = httpUrl;
        _functionAppKey = functionAppKey;
    }
    
    public async Task<Models.MovieList> GetFavoriteMoviesAsync(int page)
    {
        var response = await _client.GetPopularMoviesAsync(page, _functionAppKey);
        return ClientToUi.Map(response);
    }
}