using NProducts.Data.Context;
using NProducts.Data.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NProducts.DAL.Interfaces;
using System.Linq;
using System.Linq.Expressions;
using NProducts.Data.Common;

namespace NProducts.DAL.Repository
{
    /// <summary>
    /// 
    /// </summary>
    public class SuppliersRepository : IRepository<Suppliers>
    {
        private NorthwindContext db;

        /// <summary>
        /// Initializes a new instance of the <see cref="SuppliersRepository"/> class.
        /// </summary>
        /// <param name="db">The database.</param>
        public SuppliersRepository(NorthwindContext db)
        {
            this.db = db;
        }

        public void Create(Suppliers item)
        {
            db.Suppliers.Add(item);
        }

        public void Delete(int id)
        {
            Suppliers item = db.Suppliers.Find(id);
            if (item != null)
                db.Suppliers.Remove(item);
        }

        public IEnumerable<Suppliers> Find(Func<Suppliers, bool> predicate)
        {
            return db.Suppliers.Where(predicate).ToList();
        }       

        public Suppliers Get(int id)
        {
            return this.db.Suppliers.Find(id);
        }

        public async Task<Suppliers> GetAsync(int id)
        {
            return await this.db.Suppliers.FindAsync(id);
        }

        public IEnumerable<Suppliers> GetAll()
        {
            return this.db.Suppliers;
        }

        public async Task<IEnumerable<Suppliers>> GetAllAsync()
        {
            return await this.db.Suppliers.ToListAsync();
        }

        public void Update(Suppliers item)
        {
            db.Entry(item).State = EntityState.Modified;
        }        

        public async Task<IEnumerable<Suppliers>> GetAsync(int page, int pagesize, string orderbyfieldname, string orderbydirection, Func<Suppliers, bool> filter)
        {
            if (string.IsNullOrEmpty(orderbyfieldname))
            {
                var p = new Suppliers();
                orderbyfieldname = nameof(p.SupplierId);
                orderbydirection = "ASC";
            }

            var query = from t in this.db.Suppliers select t;
            query = query.Where(a => filter(a));
            if (orderbydirection == "ASC")
            {
                query = query.OrderByProperty(orderbyfieldname);
            }
            else
            {
                query = query.OrderByPropertyDescending(orderbyfieldname);
            }

            query = query.Skip((page - 1) * page);
            query = query.Take(pagesize);
            return await query.ToListAsync();
        }
    }
}