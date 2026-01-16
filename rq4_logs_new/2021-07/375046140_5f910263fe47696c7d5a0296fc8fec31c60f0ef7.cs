using System;
using System.Threading.Tasks;
using Core.Entities;

namespace Core.Interfaces
{
    /// <summary>
    /// Main station to management entities
    /// Sometimes is required save data inside one method to differents tables
    /// This class collect all tables to saving and with complete method data are inserted
    /// </summary>
    public interface IUnitOfWork : IDisposable
    {
        /// <summary>
        /// Reference to all common sql methods
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns>Query defined on specific entity</returns>
        IGenericRepository<TEntity> Repository<TEntity>() where TEntity : BaseEntity;
        
        /// <summary>
        /// Save all collected entities to database
        /// </summary>
        /// <returns>Number of entities saved in database if successful</returns>
        Task<int> CompleteAsync();
    }
}