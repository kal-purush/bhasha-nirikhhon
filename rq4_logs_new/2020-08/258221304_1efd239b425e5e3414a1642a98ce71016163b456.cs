using GameSource.Data.Repositories.GameSource.Contracts;
using GameSource.Models.GameSource;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;

namespace GameSource.Data.Repositories.GameSource
{
    public class NewsArticleRepository : BaseRepository<NewsArticle>, INewsArticleRepository
    {
        private GameSource_DBContext context;
        private DbSet<NewsArticle> entity;

        public NewsArticleRepository(GameSource_DBContext context) : base(context)
        {
            this.context = context;
            entity = context.Set<NewsArticle>();
        }

        public IEnumerable<NewsArticle> GetAll()
        {
            return entity.ToList();
        }

        public NewsArticle GetByID(int id)
        {
            return entity.Find(id);
        }

        public void Insert(NewsArticle newsArticle)
        {
            entity.Add(newsArticle);
            context.SaveChanges();
        }

        public void Update(NewsArticle newsArticle)
        {
            entity.Update(newsArticle);
            context.SaveChanges();
        }

        public void Delete(int id)
        {
            var newsArticle = GetByID(id);
            entity.Remove(newsArticle);
            context.SaveChanges();
        }
    }
}