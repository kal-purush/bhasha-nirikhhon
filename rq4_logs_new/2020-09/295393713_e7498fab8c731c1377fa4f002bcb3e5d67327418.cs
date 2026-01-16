using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SpaceParkBackend.Models;
using SpaceParkBackend.Database;

namespace Social.API.Services
{
    public class Repository<T> : IRepository<T> where T : class
    {
        protected readonly SpaceparkContext _context;
        protected readonly ILogger<Repository<T>> _logger;
        private DbSet<T> table = null;
        public Repository(SpaceparkContext _context, ILogger<Repository<T>> logger)
        {
            this._context = _context;
            table = _context.Set<T>();
            _logger = logger;
        }

        public async Task Add(T entity)
        {
            _logger.LogInformation($"Adding object of type {entity.GetType()}");
            await table.AddAsync(entity);
        }

        public void Update(T entity)
        {
            _logger.LogInformation($"Updating object of type {entity.GetType()}");
            _context.Update(entity);
        }

        public void Delete(T entity)
        {
            _logger.LogInformation($"Deleting object of type {entity.GetType()}");
            table.Remove(entity);
        }

        public async Task<bool> Save()
        {
            _logger.LogInformation("Saving changes");
            return (await _context.SaveChangesAsync()) >= 0;
        }
    }
}