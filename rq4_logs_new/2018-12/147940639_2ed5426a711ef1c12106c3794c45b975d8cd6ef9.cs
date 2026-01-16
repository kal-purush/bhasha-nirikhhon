using Articles.Core.Models;
using System.Linq;

namespace Articles.Core.Extensions
{
    public static class ArticlesExtensions
    {
        public static IQueryable<Shared.Core.Models.Articles> ApplyFilter(this IQueryable<Shared.Core.Models.Articles> articles, ArticlesFilter filter)
        {
            if (filter == null)
                return articles;

            if (filter.IsActive.HasValue)
                articles = articles.Where(c => c.IsActive == filter.IsActive);

            return articles;
        }
    }
}