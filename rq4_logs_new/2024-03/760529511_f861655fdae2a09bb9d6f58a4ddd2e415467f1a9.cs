using Microsoft.EntityFrameworkCore;
using Models.EntityFramework;
using Repository.EntityFramework.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Repository.EntityFramework.Concrete
{
    public abstract class EntityFrameworkRepository<TEntity>
        : IEntityRepository<TEntity>
         where TEntity : BaseEntity,
         new()
    {
        private readonly DbContext _context;
        public EntityFrameworkRepository(DbContext context) { _context = context; }

        public async Task<TEntity?> Add(TEntity entity)
        {
            entity.DateCreated = DateTime.Now;
            entity.DateLastModified = DateTime.Now;
            entity.Active = true;

            var addedEntity = await _context.AddAsync(entity);
            var affected = await _context.SaveChangesAsync();
            if (affected < 1)
                return null;

            return addedEntity.Entity;
        }

        public Task<TEntity?> Get(Expression<Func<TEntity, bool>> filter)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<TEntity>?> GetList(Expression<Func<TEntity, bool>> filter)
        {
            throw new NotImplementedException();
        }

        public Task<EntityState> Update(TEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}