using AuthApi.Models;
using Microsoft.EntityFrameworkCore;

namespace AuthApi.Context
{
    public class DepartmentDbContext: DbContext
    {
        public DepartmentDbContext(DbContextOptions<DepartmentDbContext> options): base(options)
        {
            
        }

        public DbSet<Departments> Departments { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Departments>().ToTable("departments");
        }
    }
}