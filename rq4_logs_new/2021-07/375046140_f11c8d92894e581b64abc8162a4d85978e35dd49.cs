using System.Threading.Tasks;
using Core.Entities;
using Core.Specifications;

namespace Core.Interfaces
{
    /// <summary>
    /// Common methods for all entities to retrieve data
    /// </summary>
    /// <typeparam name="T">The entity</typeparam>
    public interface IGenericRepository<T> where T : BaseEntity
    {
        /// <summary>
        /// Select * from T where with specification
        /// </summary>
        /// <returns>Single row from db of specific T</returns>
        Task<T> GetEntityWithSpecAsync( ISpecification<T> spec );
    }
}