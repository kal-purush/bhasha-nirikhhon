using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore;
using Data.Data;

namespace API.Utils
{
    public class OnlineLibriaryDBContextFactory : IDesignTimeDbContextFactory<OnlineLibriaryDBContext>
    {
        public OnlineLibriaryDBContext CreateDbContext(string[] args)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                           .SetBasePath(Directory.GetCurrentDirectory())
                           .AddJsonFile("appsettings.json")
                           .Build();

            var builder = new DbContextOptionsBuilder<OnlineLibriaryDBContext>();
            var connectionString = configuration.GetConnectionString("DefaultConnection");

            builder.UseNpgsql(connectionString, sqlServerOptions => sqlServerOptions.EnableRetryOnFailure());

            return new OnlineLibriaryDBContext(builder.Options);
        }
    }

}