using System.Linq.Expressions;

namespace ExpenSpend.Repository.Contracts
{
    public interface IRepository<T> where T : class
    {
        // Retrieve all entities
        Task<IEnumerable<T>> GetAllAsync();

        // Retrieve entities using a predicate
        Task<IEnumerable<T>> FindAsync(Expression<Func<T, bool>> predicate);

        // Retrieve an entity by id
        Task<T> GetByIdAsync(int id);

        // Retrieve an entity by id
        Task<T> GetByIdAsync(Guid id);

        // Retrieve a single entity using a predicate
        Task<T> SingleOrDefaultAsync(Expression<Func<T, bool>> predicate);

        // Retrieve a First entity using a predicate
        Task<T> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate);

        //Retrieve a single entity using a predicate
        Task<T> SingleAsync(Expression<Func<T, bool>> predicate);

        // Insert a new entity
        Task InsertAsync(T entity);

        // Insert a range of new entities
        Task InsertRangeAsync(IEnumerable<T> entities);

        // Update a range of entities
        Task UpdateRangeAsync(IEnumerable<T> entities);

        // Update an existing entity
        Task UpdateAsync(T entity);

        // Delete an entity
        Task DeleteAsync(T entity);

        // Delete a range of entities
        Task DeleteRangeAsync(IEnumerable<T> entities);

        // Check if entity exists
        Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate);

        // Save changes to the database
        Task SaveAsync();
    }
}