using MovieFiles.Api.Client.Services;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models;
using MovieFiles.Core.Models.Activity;

namespace MovieFiles.Core.Services
{
    public class RatingService : IRatingService
    {
        private readonly IRatingRepository _ratingRepository;
        private readonly IActivityRepository _activityRepository;

        public RatingService(IRatingRepository ratingRepository, IActivityRepository activityRepository)
        {
            _ratingRepository = ratingRepository;
            _activityRepository = activityRepository;
        }

        public async Task<double?> GetAverageRating(int movieId)
        {
            return await _ratingRepository.GetAverageRatingForMovieAsync(movieId);
        }

        public async Task<Rating?> GetRatingAsync(Guid userId, int movieId)
        {
            return await _ratingRepository.GetRatingAsync(userId, movieId);
        }

        public async Task<IEnumerable<Rating>> GetRatingsByUserAsync(Guid userId)
        {
            return await _ratingRepository.GetRatingsByUserAsync(userId);
        }

        public async Task<IEnumerable<Rating>> GetRatingsForMovieAsync(int movieId)
        {
            return await _ratingRepository.GetRatingsForMovieAsync(movieId);
        }

        public async Task SetRatingAsync(Rating rating)
        {
            await _ratingRepository.SetRatingAsync(rating);

            var ratingActivity = CreateRatingActivity(rating);

            await _activityRepository.AddActivity(ratingActivity);
        }

        private RatingActivity CreateRatingActivity(Rating rating)
        {
            return new RatingActivity()
            {
                Created = DateTime.Now,
                MovieId = rating.MovieId,
                RatingValue = rating.RatingValue,
                UserId = rating.UserId,
            };

        }
    }
}