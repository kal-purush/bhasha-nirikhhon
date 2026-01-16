using System;
using System.Linq.Expressions;
using DomainHistory.Entities;
using DomainHistory.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace DataAccessEF
{
	public abstract class GenericRepository<T, TDbContext> : IGenericRepository<T>
        where T : EntityBase
        where TDbContext : DbContext
    {
        protected readonly TDbContext context;
        public GenericRepository(TDbContext context)
        {
            this.context = context;
        }
        public void Add(T entity)
        {
            context.Set<T>().Add(entity);
        }
        public void AddRange(IEnumerable<T> entities)
        {
            context.Set<T>().AddRange(entities);
        }
        public IQueryable<T> Query()
        {
            return context.Set<T>();
        }
        public T? GetById(Guid? id)
        {
            return context.Set<T>().Find(id);
        }
        public T GetByIdOrDefault(Guid? id)
        {
            if (id == null) return Activator.CreateInstance<T>();

            var item = context.Set<T>().Find(id);

            return item != null ? item : Activator.CreateInstance<T>();
        }
        public void Remove(T entity)
        {
            context.Set<T>().Remove(entity);
        }
        public void RemoveRange(IEnumerable<T> entities)
        {
            context.Set<T>().RemoveRange(entities);
        }
    }
}
