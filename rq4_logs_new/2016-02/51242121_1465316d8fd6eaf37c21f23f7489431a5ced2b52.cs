using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Linq.Expressions;
using ItAcademy.PropertyCenter.Entities.Core;

namespace ItAcademy.PropertyCenter.Repository.Contracts
{
    public class GenericRepository<T> : IReadOnlyRepository<T>, IPersistRepository<T>
        where T : class, IEntityWithKey
    {
        private readonly DbContext context;
        private readonly DbSet<T> entitySet;

        public GenericRepository(DbContext context)
        {
            this.context = context;
            entitySet = context.Set<T>();
        }

        protected DbContext Context { get { return context; } }
        protected DbSet<T> EntitySet { get { return entitySet; } }

        public T GetById(int id)
        {
            return entitySet.Find(id);
        }

        public ICollection<T> GetAll()
        {
            return entitySet.ToList();
        }

        public ICollection<T> Query(Expression<Func<T, bool>> predicate)
        {
            return entitySet.Where(predicate).ToList();
        }

        public void Insert(T entity)
        {
            entitySet.Add(entity);
        }

        public void Update(T entity)
        {
            if (context.Entry(entity).State == EntityState.Detached)
            {
                entitySet.Attach(entity);
            }

            context.Entry(entity).State = EntityState.Modified;
        }

        public void Delete(T entity)
        {
            if (context.Entry(entity).State == EntityState.Detached)
            {
                entitySet.Attach(entity);
            }

            entitySet.Remove(entity);
        }

        public void Delete(int id)
        {
            T entity = GetById(id);

            Delete(entity);
        }

        private bool disposed = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    context.Dispose();
                }
            }
            this.disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}