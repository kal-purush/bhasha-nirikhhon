using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace retrowebcore.Models
{
    public interface IRepository<T>
    {

        Task<T> FirstOrDefault(Expression<Func<T, bool>> predicate, string tag = "");

        Task<List<T>> FindAll(Expression<Func<T, bool>> predicate, string tag = "");

        Task<List<T>> FindAll(RepoQuery<T> q, string tag = "");

        Task SaveChanges();
    }

    public class RepoQuery<T> 
    {
        public Expression<Func<T, bool>> Predicate { get; set; }
        public int Skip { get; set; }
        public int Take { get; set; }

        public Expression<Func<Board, object>> OrderAscending { get; set;  }
    }
}