using Microsoft.EntityFrameworkCore;
using ProjectMVC.BLL.Interfacies;
using ProjectMVC.DAL.Data;
using ProjectMVC.DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectMVC.BLL.Repositories
{
    public class GenericRepository<T> : IGenericRepository<T> where T : ModelBase
    {
        private protected readonly AppDbContext _dbcontext;

        public GenericRepository(AppDbContext dbContext)
        {
            _dbcontext = dbContext;
        }

        public int Add(T item)
        {
            _dbcontext.Set<T>().Add(item);
            //_dbcontext.Add(item);
            return _dbcontext.SaveChanges();
        }

        public int Delete(T item)
        {
            _dbcontext.Set<T>().Remove(item);
            return _dbcontext.SaveChanges();
        }

        public IEnumerable<T> GetAll()
        {
            return _dbcontext.Set<T>().AsNoTracking().ToList();
        }

        public T GetById(int id)
        {
            return _dbcontext.Set<T>().Find(id);
        }

        public int Update(T item)
        {
            _dbcontext.Set<T>().Update(item);
            return _dbcontext.SaveChanges();
        }
    }
}