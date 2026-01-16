using System.Linq.Expressions;

namespace EtudeManyToMany.API.Repository
{
    public interface IRepository<TEntity>
    {
        //CREATE
        Task<int> Add(TEntity contact);
        // READ
        Task<TEntity?> GetById(int id);
        Task<TEntity?> Get(Expression<Func<TEntity, bool>> predicate);
        Task<List<TEntity>> GetAll();
        Task<List<TEntity>> GetAll(Expression<Func<TEntity, bool>> predicate);
        // UPDATE
        Task<bool> Update(TEntity contact);
        // DELETE
        Task<bool> Delete(int id);
    }
}