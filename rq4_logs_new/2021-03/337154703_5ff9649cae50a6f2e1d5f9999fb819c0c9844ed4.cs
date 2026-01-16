using Microsoft.EntityFrameworkCore;
using TennisClub.Data.model;

namespace TennisClub.Data.context
{
    public class InMemoryDbContext : DbContext
    {
        public DbSet<ChildInDb> ChildDbSet { get; private set; }
        public DbSet<GroupInDb> GroupDbSet { get; private set; }
        
        public InMemoryDbContext(DbContextOptions<InMemoryDbContext> options) : base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ChildInDb>()
            .HasKey(t => t.Id);
            
            modelBuilder.Entity<GroupInDb>()
                .HasKey(t => t.Id);
        }
    }
}