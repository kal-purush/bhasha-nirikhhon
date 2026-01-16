using System.Linq.Expressions;

namespace Lucky7_Inventory_System_Application.Interfaces;

public interface IGenericRepository<TEntity> where TEntity : class 
{
    Task<TEntity> GetById(int id);

    Task<IEnumerable<TEntity>> GetAll();

    Task<TEntity> Add(TEntity entity);

    Task<TEntity> Update(TEntity entity);

    Task Delete(TEntity entity);

    Task<IEnumerable<TEntity>> GetWhere(Expression<Func<TEntity, bool>> predicate);

    Task<TEntity> GetSingleWhere(Expression<Func<TEntity, bool>> predicate);
}