using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;
using ShortSharing.DAL.Constants;
using ShortSharing.DAL.Context;

namespace ShortSharing.DAL.Common
{
    public class ApplicationDbContextFactory : IDesignTimeDbContextFactory<ApplicationDbContext>
    {
        public ApplicationDbContext CreateDbContext(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Path.Combine(Directory.GetCurrentDirectory(), "../ShortSharing.API/ShortSharing.API/"))
                .AddJsonFile("appsettings.json")
                .Build();

            var options = new DbContextOptionsBuilder<ApplicationDbContext>();
            options.UseNpgsql(configuration.GetConnectionString(DataAccessConstants.DbConnection));

            var context = new ApplicationDbContext(options.Options);

            return context;
        }
    }
}