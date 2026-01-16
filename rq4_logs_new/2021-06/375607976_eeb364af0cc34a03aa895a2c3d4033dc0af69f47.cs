using Microsoft.EntityFrameworkCore;
using System.Threading.Tasks;
using WebAppCQRS.Models;

namespace WebAppCQRS.Context
{
    public class ApplicationContext : DbContext, IApplicationContext
    {
        public DbSet<Product> Products { get; set; }
        public ApplicationContext(DbContextOptions<ApplicationContext> options)
            : base(options)
        { }
        public async Task<int> SaveChangesAsync() => await base.SaveChangesAsync();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Product>(entity =>
            {
                entity.Property(e => e.BuyingPrice).HasPrecision(18, 2);
                entity.Property(e => e.Rate).HasPrecision(18, 2);
            });
        }
    }
}