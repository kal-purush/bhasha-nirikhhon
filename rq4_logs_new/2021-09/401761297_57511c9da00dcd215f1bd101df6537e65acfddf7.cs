using Disney.Infrastructure.Interfaces;
using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;

namespace Disney.Infrastructure.Repositories
{
    public class BaseRepository<T> : IBaseRepository<T> where T : class
    {
        #region Context and Constructor

        private readonly DataContext context;
        public BaseRepository(DataContext context)
            => this.context = context;
        #endregion

        public async Task<IQueryable<T>> GetAll()
        {
            var response = await context.Set<T>()
                .ToListAsync();

            return (IQueryable<T>)response;
        }
        public async Task<T> GetbyId(int id)
        {
            var response = await context.Set<T>()
                .FindAsync(id);

            return response;
        }
        public T GetbyName(string name)
        {
            var response = context.Set<T>()
                .Find(name);

            return response;
        }
        public async Task Create(T entity)
        {
            await context.Set<T>().AddAsync(entity);
            await context.SaveChangesAsync();
        }
        public async Task Update(T entity)
        {
            context.Set<T>().Update(entity);
            await context.SaveChangesAsync();
        }
    }
}