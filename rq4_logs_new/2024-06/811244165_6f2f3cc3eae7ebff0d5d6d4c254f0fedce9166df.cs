using HealthMate.DAL.Abstractions;
using HealthMate.DAL.DbContexts;
using Microsoft.EntityFrameworkCore;

namespace HealthMate.DAL.Repositories
{
    internal class GenericRepository<TEntity> : IGenericRepository<TEntity> where TEntity : BaseEntity
    {
        protected readonly ApplicationDbContext _context;

        public async Task<TEntity?> GetById(Guid id, CancellationToken token) =>
            await _context.Set<TEntity>().Where(t => t.Id.Equals(id)).SingleOrDefaultAsync(token);

        public async Task<ICollection<TEntity>> GetAll(CancellationToken token) =>
            await _context.Set<TEntity>().ToListAsync(token);

        public async Task<TEntity> Update(TEntity entity, CancellationToken token)
        {
            _context.Entry(entity).State = EntityState.Modified;
            await _context.SaveChangesAsync(token);
            return entity;
        }

        public async Task Delete(Guid id, CancellationToken token)
        {
            var entity = await _context
                .Set<TEntity>()
                .AsNoTracking()
                .Where(t => t.Id.Equals(id))
                .SingleOrDefaultAsync(token);

            if (entity != null)
            {
                _context.Set<TEntity>().Remove(entity);

                await _context.SaveChangesAsync(token);
            }
        }

        public async Task<TEntity> Create(TEntity entity, CancellationToken token)
        {
            _context.Set<TEntity>().Add(entity);
            await _context.SaveChangesAsync(token);
            return entity;
        }
    }
}