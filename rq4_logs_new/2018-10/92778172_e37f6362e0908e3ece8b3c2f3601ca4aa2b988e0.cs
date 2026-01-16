using System;
using System.Collections.Generic;
using System.Linq;
using LifeLike.Data.Models;
using LifeLike.Data.Models.Enums;

namespace LifeLike.Data
{
    public class UnitOfWork : IUnitOfWork 
    {
        public UnitOfWork(PortalContext db)
        {
            this.db= db;
        }
        // Słownik będzie używany do sprawdzania instancji repozytoriów
        private Dictionary<Type, object> repositories = new Dictionary<Type, object>();
        public IRepository<T> Get<T>() where T : class
        {
            // Jeżeli instancja danego repozytorium istnieje - zostanie zwrócona
            if (repositories.Keys.Contains(typeof(T)) == true)
                return repositories[typeof(T)] as IRepository<T>;
            // Jeżeli nie, zostanie utworzona nowa i dodana do słownika
            IRepository<T> repo = new Repository<T>(db);
            repositories.Add(typeof(T), repo);
            return repo;
        }
        public Result Save()
        {
            try
            {
                db.SaveChanges();
                return Result.Success;
            }
            catch(Exception e)
            {
                return Result.Failed;
            }
        }
        private readonly PortalContext db;
    }
       
}