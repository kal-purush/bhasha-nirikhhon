using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Simply_Gallery.Interfaces
{
    public interface IRepository<T> where T : class
    {
        Task<IQueryable<T>> GetAllAsync(Expression<Func<T, bool>> predicate);
        Task<T> GetAsync(Expression<Func<T, bool>> predicate);
        Task<T> AddAsync(T item);
        Task<T> UpdateAsync(T item);
        Task DeleteAsync(Expression<Func<T, bool>> predicate);
        Task<bool> IsFindAsync(Expression<Func<T, bool>> predicate);
    }
}