namespace BoardGames.Services.Data
{
    using BoardGames.Data.Common;
    using BoardGames.Data.Models;
    using System;
    using System.Linq;
    public class SatisticsService : IStatisticsService
    {
        private readonly IDbRepository<Tutorial> tutorials;
        private readonly IDbRepository<Review> reviews;
        private readonly IDbRepository<Comment> comments;
        private readonly IDbRepository<Like> likes;

        public SatisticsService(IDbRepository<Tutorial> tutorials, IDbRepository<Review> reviews, IDbRepository<Comment> comments, IDbRepository<Like> likes)
        {
            this.tutorials = tutorials;
            this.reviews = reviews;
            this.comments = comments;
            this.likes = likes;
        }

        public int[] GetStatistics()
        {
            var reviewsCount = this.reviews.All().Count();
            var tutorialsCount = this.tutorials.All().Count();
            var commentsCount = this.comments.All().Count();
            var likesCount = this.likes.All().Count();

            return new int[] { reviewsCount, tutorialsCount, commentsCount, likesCount };
        }
    }
}