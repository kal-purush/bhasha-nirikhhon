using System.Linq.Expressions;

namespace Application.Contract.Repository
{
    public interface IGenericRepository<T> where T : class
    {
        Task<IEnumerable<T>> GetAllAsTrackingAsync();
        Task<IEnumerable<T>> GetAllAsNoTrackingAsync();
        Task<T?> GetByIdAsync(params object[] keyValues);
        Task<T?> Find(Expression<Func<T, bool>> expression);
        Task<T?> Find(Expression<Func<T, bool>> expression, string[] includes);
        Task<IEnumerable<T>> FindAll(Expression<Func<T, bool>> expression);
        Task<IEnumerable<T>> FindAll(Expression<Func<T, bool>> expression, string[] includes);
        Task<T> AddAsync(T entity);
        T Update(T entity);
        T Delete(T entity);
        IQueryable<T> GetTableNoTrackingQuery();
        IQueryable<T> GetTableAsTrackingQuery();
        Task<bool> ExistsAsync(Expression<Func<T, bool>> expression);
    }
}