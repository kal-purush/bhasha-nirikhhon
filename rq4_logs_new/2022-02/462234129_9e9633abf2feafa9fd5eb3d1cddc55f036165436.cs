using DAL.Entities;
using Microsoft.EntityFrameworkCore;

namespace DAL.EF
{
    public class CatDbContext : DbContext
    {
        public DbSet<Cat> Cats { get; set; }

        public CatDbContext(DbContextOptions<CatDbContext> options)
        : base(options)
        {
            Database.EnsureCreated();
        }
    }
}