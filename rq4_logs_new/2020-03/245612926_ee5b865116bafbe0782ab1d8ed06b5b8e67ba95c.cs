using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using CourseGenerator.DAL.Pagination;
using CourseGenerator.DAL.Context;
using CourseGenerator.BLL.Interfaces;

namespace CourseGenerator.BLL.Repositories
{
    public class GenericEFRepository<T> : IGenericEFRepository<T> where T : class
    {
        protected readonly ApplicationContext _context;
        protected readonly DbSet<T> _set;

        public GenericEFRepository(ApplicationContext context)
        {
            _context = context;
            _set = _context.Set<T>();
        }


        public async Task<T> GetAsync(params object[] key) => await _set.FindAsync(key);

        public async Task<IEnumerable<T>> GetAllAsync() => await _set.ToListAsync();

        public IQueryable<T> GetAllQueryable() => _set.AsQueryable();
        public async Task<PagedList<T>> GetPagedAsync(int pageSize, int pageIndex)
        {
            return await _set.ToPagedListAsync(pageSize, pageIndex);
        }


        public async Task CreateAsync(T item) => await _set.AddAsync(item);

        public void Update(T item) => _set.Update(item);

        public void Delete(T item) => _set.Remove(item);

        public void Dispose() => _context.DisposeAsync();
    }
}