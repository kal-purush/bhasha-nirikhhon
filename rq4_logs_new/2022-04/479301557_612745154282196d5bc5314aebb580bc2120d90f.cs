using LinkBakery.Core.Data;
using LinkBakery.Core.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace LinkBakery.Core.Repositories
{
    public class BaseRepository<T> : IBaseRepository<T> where T : class
    {
        protected readonly ApplicationDbContext _context;
        protected readonly DbSet<T> _table;


        public BaseRepository(IConfiguration configuration)
        {
            var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
            optionsBuilder.UseSqlServer(configuration.GetConnectionString("DefaultConnection"));

            _context = new ApplicationDbContext(optionsBuilder.Options);
            _table = _context.Set<T>();
        }



        public IEnumerable<T> GetAll()
        {
            return _table.ToList();
        }

        public T? GetById(object id)
        {
            return _table.Find(id);
        }

        public void Insert(T entity)
        {
            _table.Add(entity);
        }

        public void Update(T entity)
        {
            _table.Attach(entity);
            _context.Entry(entity).State = EntityState.Modified;
        }

        public void Delete(object id)
        {
            T? existing = _table.Find(id);

            if (existing != null)
            {
                _table.Remove(existing);
            }
        }

        public void SaveAsync()
        {
            _context.SaveChangesAsync();
        }
    }
}