using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Data.Models.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace Data.Repositories
{
    public class GenericRepository<T> : IGenericRepository<T> where T :class
    {
        private readonly AppDbContext _context;
        private DbSet<T> _table;
        public GenericRepository(AppDbContext context)
        {
            _context = context;
            _table = context.Set<T>();
        }

        public async Task Add(T entity)
        {
            await _table.AddAsync(entity);
            await _context.SaveChangesAsync();
        }

        public async Task  Delete(int Id)
        {
            T table = _table.Find(Id);
             _table.Remove(table);
            await _context.SaveChangesAsync();
        }

        public async Task<T> FindById(int Id)
        {
            return await _table.FindAsync(Id);
        }

        public async Task<List<T>> GetAll()
        {
            return await _table.ToListAsync();

        }
        public async Task Save()
        {
            await _context.SaveChangesAsync();
        }
        public void Update(T entity)
        {
            _table.Attach(entity);
            _context.Entry(entity).State = EntityState.Modified;
        }
    }
}