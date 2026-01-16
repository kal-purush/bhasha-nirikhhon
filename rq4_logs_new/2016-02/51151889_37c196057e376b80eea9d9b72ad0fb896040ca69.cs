namespace BoardGames.Services.Data
{
    using System;
    using BoardGames.Data.Common;
    using BoardGames.Data.Models;
    using BoardGames.Services.Data.Contracts;
    using System.Linq;
    public class LikesService : ILikesService
    {
        private readonly IDbRepository<Like> likes;

        public LikesService(IDbRepository<Like> likes)
        {
            this.likes = likes;
        }

        public void Add(int tutorialId, string userId)
        {
            var newLike = new Like
            {
                UserId = userId,
                TutorialId = tutorialId
            };

            this.likes.Add(newLike);
            this.likes.Save();
        }

        public void Delete(int id)
        {
            var likeToBeDeleted = this.likes.GetById(id);

            this.likes.HardDelete(likeToBeDeleted);
            this.likes.Save();
        }

        public Like GetByUserIdAndTutorialId(int tutorialId, string userId)
        {
            return this.likes.All().Where(l => l.UserId == userId && l.TutorialId == tutorialId).FirstOrDefault();
        }

        public int GetCount()
        {
            return this.likes.All().Count();
        }
    }
}