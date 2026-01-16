using Microsoft.EntityFrameworkCore;
using Photos.Domain.DataBaseEntity.Domain;
using Photos.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Photos.Application.Repositories
{
    public abstract class DataBaseRepository<TEntity, TKey> : IDataBaseRepository<TEntity, TKey>
        where TEntity : class, IEntity, new()
        where TKey : struct
    {
        protected readonly PhotosDBContext _photosDBContext;

        public DataBaseRepository(PhotosDBContext photosDBContext)
        {
            _photosDBContext = photosDBContext ?? throw new ArgumentNullException(nameof(photosDBContext));
        }

        public async Task AddAsync(TEntity entity, CancellationToken cancellationToken = default)
        {
            await _photosDBContext.Set<TEntity>().AddAsync(entity, cancellationToken);
        }

        public async Task<TEntity> FindAsync(TKey key, CancellationToken cancellationToken = default)
        {
            return await _photosDBContext.Set<TEntity>().FindAsync(new object[] { key }, cancellationToken);
        }

        public async Task<IEnumerable<TEntity>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            return await _photosDBContext.Set<TEntity>().ToListAsync(); 
        }

        public void Remove(TEntity entity)
        {
            if (entity == null) return;
            _photosDBContext.Set<TEntity>().Remove(entity);
        }
             
        public void Update(TEntity entity)
        {
            _photosDBContext.Entry(entity).State = EntityState.Modified;
        }
    }
}