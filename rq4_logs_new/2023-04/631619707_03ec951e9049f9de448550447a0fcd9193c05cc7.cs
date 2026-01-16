using PocketForzaHorizonCommunity.Back.Database.Entities;

namespace PocketForzaHorizonCommunity.Back.Database.Repos;

public class RepositoryBase<TEntity> where TEntity : EntityBase
{
    protected ApplicationDbContext Context { get; }

    public RepositoryBase(ApplicationDbContext context)
    {
        Context = context;
    }

    public async Task<int> SaveAsync() => await Context.SaveChangesAsync();

    public IQueryable<TEntity> GetAll() => Context.Set<TEntity>().AsQueryable<TEntity>();

    public IQueryable<TEntity> GetById(Guid id) => GetAll().Where(i => i.Id == id);

    public async virtual Task Create(TEntity newEntity) => await Context.Set<TEntity>().AddAsync(newEntity);

    public virtual void Delete(TEntity entity) => Context.Set<TEntity>().Remove(entity);
}