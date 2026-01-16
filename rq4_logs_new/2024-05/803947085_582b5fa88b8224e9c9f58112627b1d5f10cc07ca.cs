using Event_Planinng_System_DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
namespace Event_Planinng_System_DAL.Repos
{
    public class GenericRepo<TEntity> where TEntity : class
    {
        dbContext db;
        public GenericRepo(dbContext _db)
        {
            db = _db;
        }

        public List<TEntity> GetAll()
        {
            return db.Set<TEntity>().ToList();
        }

        public void Add(TEntity entity)
        {
            db.Set<TEntity>().Add(entity);
        }

        public void Edit(TEntity entity)
        {
            db.Entry(entity).State = EntityState.Modified;
        }

        public void HardDelete(TEntity entity)
        {
            db.Set<TEntity>().Remove(entity);
        }
    }
}