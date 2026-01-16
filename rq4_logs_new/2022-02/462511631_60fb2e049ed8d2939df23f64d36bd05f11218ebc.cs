using Microsoft.EntityFrameworkCore;

namespace SOSH3.Museum.Database.Tests
{
    public class InMemoryDbContext : AppDbContext
    {
        private readonly string dbName = Guid.NewGuid().ToString();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseInMemoryDatabase(dbName);
        }
    }
}