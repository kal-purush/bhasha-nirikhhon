using Event_Planinng_System_DAL.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Event_Planinng_System_DAL.Repos
{
    public class GenericRepoForId <TEntity>: GenericRepo<TEntity> where TEntity : InheritIdAndIsDeleted
    {

        public GenericRepoForId(dbContext _db) : base(_db){}

        public async Task<TEntity> FindById(int id)
        {
            var entity = await db.Set<TEntity>().FindAsync(id);
            if (entity == null)
            {
                throw new Exception("Fuck You");
            }
            return entity;
        }

        public override async Task Delete(TEntity entity)
        {
            entity.IsDeleted = true;
            db.Entry(entity).State = EntityState.Modified;
            await db.SaveChangesAsync();
        }
    }
}