using System.Reflection;
using HealthMate.DAL.DbContexts;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace HealthMate.DAL.ContextFactory
{
    public class ApplicationContextFactory : IDesignTimeDbContextFactory<ApplicationDbContext>
    {
        public ApplicationDbContext CreateDbContext(string[] args)
        {
            var builder = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseNpgsql("Server=localhost;Port=5432;User Id=postgres;Password=12345;Database=HealthMateDb",
            b => b.MigrationsAssembly(Assembly.GetExecutingAssembly().FullName));

            return new ApplicationDbContext(builder.Options);
        }
    }
}