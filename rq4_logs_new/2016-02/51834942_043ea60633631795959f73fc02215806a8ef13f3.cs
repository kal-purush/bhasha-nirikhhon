using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SignalR.Data.Infrastructure;

namespace Wunderlist.Service.Infrastructure
{
    public class BaseService<T> : IService<T> where T : class
    {
        protected IUnitOfWork UOWork { get; set; }
        protected IRepository<T> Rep { get; set; }

        public BaseService(IUnitOfWork uoW,IRepository<T> rep )
        {
            this.Rep = rep;
            this.UOWork = uoW;
        }

        public void Add(T entity)
        {
           Rep.Add(entity);
           UOWork.Commit();
        }

        public void Delete(T entity)
        {
            Rep.Delete(entity);
            UOWork.Commit();   
        }

        public IEnumerable<T> GetAll()
        {
            return Rep.GetAll();
        }

        public T GetById(string id)
        {
            return Rep.GetById(id);
        }

        public T GetById(long id)
        {
            return Rep.GetById(id);
        }

        public void Update(T entity)
        {
            Rep.Update(entity);
            UOWork.Commit();
        }
    }
}