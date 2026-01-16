namespace BoardGames.Services.Data.Contracts
{
    using BoardGames.Data.Models;
    using System.Linq;

    public interface IReviewsService
    {
        IQueryable<Review> GetAll();

        void Add(string gameTitle, int categoryId, string content, int minPlayers, int maxPlayers, int minAge, int minTime, string siteUrl, string creatorId);

        void Edit(int id, string gameTitle, int categoryId, string content, int minPlayers, int maxPlayers, int minAge, int minTime, string siteUrl);

        void Delete(int id);

        Review GetById(int id);
    }
}