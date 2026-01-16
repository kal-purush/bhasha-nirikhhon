using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ScheduleSystem.DataSource.Abstractions.Contracts;
using ScheduleSystem.Domain.Entities;

namespace ScheduleSystem.DataSource.Implementation.Base
{
	public abstract class RepositoryBase<TEntity> : IRepository<TEntity>
			where TEntity : class, IEntity
	{
		private readonly ScheduleDbContext _dbContext;

		protected DbSet<TEntity> DbSet => _dbContext.Set<TEntity>();

		protected RepositoryBase(ScheduleDbContext dbContext)
		{
			_dbContext = dbContext;
		}

		public Task<TEntity> GetByIdAsync(int id)
		{
			return DbSet.FirstOrDefaultAsync(e => e.Id == id);
		}

		public async Task<ICollection<TEntity>> GetAllAsync()
		{
			return await DbSet.ToListAsync();
		}

		public Task CreateAsync(TEntity entity)
		{
			return DbSet.AddAsync(entity);
		}

		public Task UpdateAsync(TEntity entity)
		{
			DbSet.Update(entity);
			
			return Task.CompletedTask;
		}

		public Task DeleteAsync(TEntity entity)
		{
			DbSet.Remove(entity);

			return Task.CompletedTask;
		}

		public Task SaveChangesAsync()
		{
			return _dbContext.SaveChangesAsync();
		}
	}
}