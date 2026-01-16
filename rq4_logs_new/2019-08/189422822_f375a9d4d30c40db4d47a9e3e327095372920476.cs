using DomainApp.Users;
using Microsoft.EntityFrameworkCore;

namespace ControllerApp
{
    public class DatabaseContext : DbContext
    {
        public DatabaseContext(DbContextOptions options) : base(options)
        {
        }

        public DbSet<User> Users { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<User>().HasData(new User
            {
                UserId = 1,
                Name = "Stanley",
                Surname = "Mohlala",
                Email = "Stanzo360@outlook.com",                

            });
        }
    }
}