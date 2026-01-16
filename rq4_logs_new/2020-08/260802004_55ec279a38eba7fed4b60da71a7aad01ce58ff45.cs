using Core.Entities;
using eCommerce.Infrastructure.Shared;
using System;
using System.Collections.Generic;
using System.Text;

namespace eCommerce.Infrastructure.Data
{
    public class UnitOfWork : IDisposable
    {
        private AppDbContext context { get; }
        private GenericRepository<User> userRepository; 
        
        public GenericRepository<User> GameRepository
        {
            get
            {
                return this.userRepository ?? new GenericRepository<User>(context);
            }
        }

        public void SaveChanges()
        {
            context.SaveChanges();
        }

        private bool disposed = false; 

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    context.Dispose();
                }
            }
            this.disposed = true;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}