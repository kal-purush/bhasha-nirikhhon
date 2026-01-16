using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Data.Infrastructure
{
    public abstract class DbContextRepository<T> : IRepository<T> where T : class
    {
        protected readonly DbContext DbContext;

        protected DbContextRepository(DbContext ctx) => DbContext = ctx;


        public async Task CreateDBAsync(CancellationToken ct = default)
        {
            bool canConnect = DbContext.Database.CanConnect();
            if (!canConnect) return;

            DbContext.Database.EnsureCreatedAsync();
        }

        public Task Clear(CancellationToken ct = default)
        {
            DbContext.RemoveRange(DbContext.Set<T>());
            return DbContext.SaveChangesAsync(ct);
        }

        public Task<T> GetAsync(Expression<Func<T, bool>> condition) =>
            DbContext.Set<T>().FirstOrDefaultAsync(condition);

        public virtual ValueTask<T> GetAsync(object key, CancellationToken ct = default) =>
            DbContext.Set<T>().FindAsync(keyValues: new[] { key }, cancellationToken: ct);

        public async Task<IReadOnlyList<T>> ListAsync(CancellationToken ct = default) =>
            await DbContext.Set<T>().AsNoTracking().ToListAsync(cancellationToken: ct);

        public IQueryable<T> AsQueryable() => DbContext.Set<T>();

        public async Task DeleteAsync(T entity, CancellationToken ct = default)
        {
            DbContext.Set<T>().Remove(entity);
            await DbContext.SaveChangesAsync(ct);
        }

        public Task DeleteAsync(IEnumerable<T> entities, CancellationToken ct = default)
        {
            DbContext.Set<T>().RemoveRange(entities);
            return DbContext.SaveChangesAsync(ct);
        }


        public Task DeleteAsync(CancellationToken ct = default)
        {
            DbContext.Set<T>().RemoveRange();
            return DbContext.SaveChangesAsync(ct);
        }

        public async Task DeleteAsync(object key, CancellationToken ct = default)
        {
            var entity = await GetAsync(key, ct);
            if (entity is not null) await DeleteAsync(entity, ct);
        }

        public Task<int> CountAsync(Expression<Func<T, bool>> condition, CancellationToken ct = default) =>
            DbContext.Set<T>().CountAsync(condition, ct);

        public Task<bool> AnyAsync(Expression<Func<T, bool>> condition = null, CancellationToken ct = default) =>
            null != condition ?
                DbContext.Set<T>().AnyAsync(condition, cancellationToken: ct) :
                DbContext.Set<T>().AnyAsync(cancellationToken: ct);

        public async Task<IList<TResult>> SelectAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken ct = default) =>
            await DbContext.Set<T>().AsNoTracking().Select(selector).ToListAsync(cancellationToken: ct);


        public IList<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) =>
             DbContext.Set<T>().AsNoTracking().Select(selector).ToList();

        public async Task<T> AddAsync(T entity, CancellationToken ct = default)
        {
            var s = await DbContext.Set<T>().AddAsync(entity, ct);
            var resultStatus = await DbContext.SaveChangesAsync(ct);

            return resultStatus == 1 ? entity : null;
        }

        public async Task UpdateAsync(T entity, CancellationToken ct = default)
        {
            DbContext.Entry(entity).State = EntityState.Modified;
            await DbContext.SaveChangesAsync(ct);
        }

        public async Task<int> AddRangeAsync(IEnumerable<T> entityList)
        {
            await DbContext.Set<T>().AddRangeAsync(entityList);
            return await DbContext.SaveChangesAsync();
        }

        public Task<int> CountAsync(ISpecification<T> spec = null, CancellationToken ct = default) =>
            null != spec ? ApplySpecification(spec).CountAsync(cancellationToken: ct) : DbContext.Set<T>().CountAsync(cancellationToken: ct);


        private IQueryable<T> ApplySpecification(ISpecification<T> spec) => SpecificationEvaluator<T>.GetQuery(DbContext.Set<T>().AsQueryable(), spec);
    }
}