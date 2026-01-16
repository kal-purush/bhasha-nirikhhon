using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAB2_2.Lib
{
    public class Repository<T> : IRepository<T> where T : class
    {
        private readonly DABHandin_2_2Entities _context;
        private IDbSet<T> entities;

        public Repository(DABHandin_2_2Entities context) 
        {
            _context = context;
        }

        public async void Create(T t)
        {
            this.Entities.Add(t);
        }

        public T Get(int id)
        {
            return this.Entities.Find(id);
        }

        public async void Update(int id, T t)
        {
        }

        public void Remove(int id)
        {
            this.Entities.Remove(Entities.Find(id));
        }

        private IDbSet<T> Entities => entities ?? (entities = _context.Set<T>());
    }
}