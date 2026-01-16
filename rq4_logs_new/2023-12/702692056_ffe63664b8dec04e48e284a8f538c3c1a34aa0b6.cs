namespace MentalHospital.DAL.Repositories;

public class GenericRepository<TEntity>(ApplicationDbContext context) : IGenericRepository<TEntity>
	where TEntity : Entity
{
	protected ApplicationDbContext Context { get; } = context;

	protected DbSet<TEntity> Set { get; } = context.Set<TEntity>();

	public virtual Task<TEntity?> Get(Guid id)
	{
		return Set.FirstOrDefaultAsync(p => p.Id == id);
	}

	public virtual async Task<IEnumerable<TEntity>> GetAll()
	{
		return await Set.ToListAsync();
	}

	public virtual async Task<TEntity> Create(TEntity entity)
	{
		entity.Id = Guid.NewGuid();

		Set.Add(entity);

		await Context.SaveChangesAsync();

		return entity;
	}

	public virtual async Task<TEntity> Update(TEntity entity)
	{
		Context.Entry(entity).State = EntityState.Modified;

		await Context.SaveChangesAsync();

		return entity;
	}

	public virtual async Task<TEntity?> Delete(Guid id)
	{
		var entity = Set.FirstOrDefault(p => p.Id == id);

		if (entity != null)
		{
			Set.Remove(entity);

			await Context.SaveChangesAsync();
		}

		return entity;
	}
}