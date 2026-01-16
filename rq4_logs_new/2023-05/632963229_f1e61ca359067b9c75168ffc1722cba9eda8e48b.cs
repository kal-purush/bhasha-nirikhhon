using MovieFiles.Api.Client.Mappers;
using MovieFiles.Core.Interfaces;

namespace MovieFiles.Api.Client.Services;

public class MovieDetailsService : BaseService,IMovieDetailsService
{

    public MovieDetailsService(string httpUrl, string functionAppKey) : base(httpUrl, functionAppKey)
    {
    }

    public async Task<Core.Models.Movie?> GetMovieDetailsAsync(int movieId)
    {
        var response = await _client.GetMovieDetailsAsync(movieId, _functionAppKey);
        return ClientToUi.Map(response);
    }

    public async Task<Core.Models.CreditList?> GetMovieCreditsAsync(int movieId)
    {
        var response = await _client.GetMovieCreditsAsync(movieId, _functionAppKey);
        return ClientToUi.Map(response);
    }
}