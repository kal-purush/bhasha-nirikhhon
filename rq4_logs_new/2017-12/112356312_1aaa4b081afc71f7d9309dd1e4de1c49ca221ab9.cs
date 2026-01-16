using System;
using DataDomain;
using Microsoft.EntityFrameworkCore;

namespace Repositorys
{
    public interface IRepository<T> where T : class
    {
        DbSet<T> GetAll();
        T GetById(Guid id);
        void Add(T t);
        void Update(T t);
        void Delete(Guid id);
        int SaveChanges();
    }
}