using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace planit_data.Repository
{
    public class GenericRepository<T> : IRepository<T> where T : class
    {
        private ApplicationContext context;
        private DbSet<T> set;

        public GenericRepository(ApplicationContext context)
        {
            this.context = context;
            this.set = context.Set<T>();
        }

        public IEnumerable<T> GetAll()
        {
            return set.ToList();
        }

        public T GetById(int id)
        {
            return set.Find(id);
        }

        public bool Insert(T obj)
        {
            T retObj = set.Add(obj);
            if (retObj != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public void Update(T obj)
        {
            set.Attach(obj);
            context.Entry(obj).State = EntityState.Modified;
        }

        public bool Delete(int id)
        {
            T obj = set.Find(id);
            if(obj!=null)
            {
                set.Remove(obj);
                return true;
            }
            else
            {
                return false;
            }
            
        }

    }
}