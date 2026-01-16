using System.IO;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;


namespace FanSoft.CadTurmas.Data.EF
{
    // https://www.learnentityframeworkcore.com/migrations
    // https://docs.microsoft.com/ef/core/miscellaneous/cli/dbcontext-creation
    public class CadTurmasContextFactory : IDesignTimeDbContextFactory<CadTurmasDataContext>
    {
        public CadTurmasDataContext CreateDbContext(string[] args)
        {
            IConfigurationRoot configuration =
                new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.Development.json")
                .Build();

            var builder = new DbContextOptionsBuilder<CadTurmasDataContext>();

            var connectionString = configuration.GetConnectionString("CadTurmasConn");

            builder.UseSqlServer(connectionString);

            return new CadTurmasDataContext(builder.Options);
        }
    }
}