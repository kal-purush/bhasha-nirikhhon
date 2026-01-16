using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSB.Repository.Entities;
using CSB.Repository.GenericRepo;
using CSB.Repository.Interfaces;

namespace CSB.Repository.Impl
{
    public class PhoneService<T> : IPhoneService<T> where T : Phone
    {
        private readonly IPhoneRepository _phoneRepostitory;

        private PhoneService(IPhoneRepository phoneRepository)
        {
            this._phoneRepostitory = phoneRepository;
        }
        public List<Phone> GetByName(string name)
        {
            if(name == Name)
            {
                throw new ArgumentException("Wrong Name");
            }
            
        }
        public int Create(string name)
        {
            dbContext.Set<T>().Add(name);
            dbContext.SaveChanges();
            return item.Id;
        }

        public bool Update(string name)
        {
            var entry = dbContext.Entry(name);
            entry.State = Microsoft.EntityFrameworkCore.EntityState.Modified;
            dbContext.SaveChanges();
            return true;
        }

        public bool Delete(string name)
        {
            var item = GetByName(name);
            if (item == null)
            {
                return false;
            }

            dbContext.Set<T>().Remove(item);
            dbContext.SaveChanges();
            return true;
        }

        public IReadOnlyCollection<T> GetAll()
        {
            return dbContext.Set<T>().ToList();
        }

        //public T GetById(int id)
        //{
        //    return dbContext.Set<T>().SingleOrDefault<T>(x => x.Id == id);
        //}
    }
}