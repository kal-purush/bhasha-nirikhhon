using MovieFiles.Api.Client.Mappers;
using System.ComponentModel;

namespace MovieFiles.Api.Client.Services
{
    public class RatingService : IRatingService
    {
        private readonly MovieFilesFunctions _client;
        private readonly string _functionAppKey;
        public RatingService(string httpUrl, string functionAppKey)
        {
            _client = new MovieFilesFunctions(new HttpClient { BaseAddress = new Uri(httpUrl) });
            _client.BaseUrl = httpUrl;
            _functionAppKey = functionAppKey;
        }


        public async Task<Models.Rating?> GetRatingAsync(Guid userId, Guid movieId)
        {
            try
            {
                return ClientToUi.Map(await _client.GetRatingAsync(userId, movieId, _functionAppKey));
            }
            //Found no rating 
            catch (ApiException e)
            {
                return null;
            }
        }
        public async Task<double?> GetAverageRating(Guid movieId)
        {
            return await _client.GetAverageRatingAsync(movieId, _functionAppKey);
        }

        public async Task<IEnumerable<Models.Rating>> GetRatingsByUserAsync(Guid userId)
        {
            return (await _client.GetRatingsByUserAsync(userId, _functionAppKey))?.Select(ClientToUi.Map) ?? new List<Models.Rating>();
        }

        public async Task<IEnumerable<Models.Rating>> GetRatingsForMovieAsync(Guid movieId)
        {
            return (await _client.GetRatingsForMovieAsync(movieId, _functionAppKey))?.Select(ClientToUi.Map) ?? new List<Models.Rating>();
        }

        public async Task SetRatingAsync(Models.Rating rating)
        {
            await _client.AddRatingAsync(_functionAppKey, UiToClient.Map(rating));
        }


    }
}