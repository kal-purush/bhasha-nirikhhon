using CustomerAppEntity;
using Microsoft.EntityFrameworkCore;

namespace CustomerAppDAL.Context
{
    public class InMemoryContext : DbContext
    {
        //In memory setup
        private static readonly DbContextOptions<InMemoryContext> Options = 
            new DbContextOptionsBuilder<InMemoryContext>().UseInMemoryDatabase("TheDB").Options;

        public DbSet<Customer> Customers { set; get; }

        //Options that we want in memory
        public InMemoryContext() : base(Options)
        {
            
        }
    }
}