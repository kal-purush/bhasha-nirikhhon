using System;
using System.Linq;
using DataDomain;
using DataPersistence;
using Microsoft.EntityFrameworkCore;

namespace Repositories
{
    public class CrudRepository<T> : IRepository<T> where T : BaseEntity
    {
        private readonly DataBaseContext _databaseService;

        public CrudRepository(DataBaseContext database)
        {
            _databaseService = database;
        }
        public DbSet<T> GetAll()
        {
            return _databaseService.Set<T>();
        }

        public T GetById(Guid id)
        {
            return _databaseService.Set<T>().FirstOrDefault(t => t.Id == id);
        }

        public void Add(T t)
        {
            _databaseService.Set<T>().Add(t);
            SaveChanges();
        }

        public void Update(T t)
        {
            _databaseService.Set<T>().Update(t);
            SaveChanges();
        }

        public void Delete(Guid id)
        {
            var t = GetById(id);
            _databaseService.Set<T>().Remove(t);
            SaveChanges();
        }

        public int SaveChanges()
        {
            return _databaseService.SaveChanges();
        }
    }
}