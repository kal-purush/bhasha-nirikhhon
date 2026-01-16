using HttpClientTest.Models;
using Microsoft.EntityFrameworkCore;

namespace HttpClientTest.DataContext
{
    public class DataBaseContext : DbContext
    {
        public DataBaseContext(DbContextOptions<DataBaseContext> options) : base(options)
        {
            
        }

        public DbSet<DrinkModel> Drinks => Set<DrinkModel>();
    }
}