using DotNetTeam7API.Interfaces;
using DotNetTeam7API.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DotNetTeam7API.Data
{
    public class BaseRepository<T> : IBaseRepository<T> where T : BaseEntity // tQ: determines the data type T, then determines what list it is
    {
        protected readonly MovieDbContext _db;

        // tQ: upon startup, because of dependency injection, dbContext referred to in StartUp is instantiated
        public BaseRepository(MovieDbContext db)
        {
            _db = db;
        }

        public IQueryable<T> GetAll()
        {
            // tQ: need to return return data set of type T
            return _db.Set<T>();
        }
    }
}