using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using QvAbu.Api.Models;
using QvAbu.Api.Models.Questions;

namespace QvAbu.Api.Data.Repository
{
    public interface IRevisionEntitesRepo
    {
        Task<object> GetAsync(Guid id, int revision);
    }

    public interface IRevisionEntitesRepo<T> : IRevisionEntitesRepo where T : RevisionEntity
    {
        Task<T> GetEntityAsync(Guid id, int revision);
    }

    public abstract class RevisionEntitesRepo<T, TContext> : Repository<T, TContext>, IRevisionEntitesRepo<T> 
        where T : RevisionEntity 
        where TContext : IDbContext
    {
        protected RevisionEntitesRepo(TContext context) : base(context)
        {
        }

        public virtual async Task<object> GetAsync(Guid id, int revision) => await this.GetEntityAsync(id, revision);
        public virtual async Task<T> GetEntityAsync(Guid id, int revision) => await this.GetEntityAsync(id, revision, queryable => queryable);

        protected async Task<T> GetEntityAsync(Guid id, int revision, Func<IQueryable<T>, IQueryable<T>> queryableFunc)
        {
            return await queryableFunc(this.Context.Set<T>())
                .FirstOrDefaultAsync(_ => _.ID == id && _.Revision == revision);
        }
    }
}