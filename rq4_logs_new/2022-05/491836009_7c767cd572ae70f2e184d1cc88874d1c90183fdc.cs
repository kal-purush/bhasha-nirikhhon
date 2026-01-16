using Data.Access.Layer.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Business.Access.Layer.Extensions
{
    public static class SqlConnector
    {
        public static void ConnectToSqlServer(IServiceCollection services, IConfiguration configuration)
        {
            services.AddDbContext<SqlDataContext>(options =>
            {
                options.UseMySql(
                   configuration.GetConnectionString("DefaultConnection"),
                   ServerVersion.AutoDetect(configuration.GetConnectionString("DefaultConnection"))
                   );
            });
        }
    }
}