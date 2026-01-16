using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Standard.Tool.Platform.Data.Infrastructure;
using Standard.Tool.Platform.PostgreSql.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.PostgreSql
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddPostgreSqlStorage(this IServiceCollection services, string connectionString)
        {
            services.AddScoped(typeof(IRepository<>), typeof(PostgreSqlDbContextRepository<>));

            AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
            services.AddDbContext<PostgreSqlToolsBlockDbContext>(optionsAction => optionsAction
                .UseLazyLoadingProxies()
                .EnableDetailedErrors()
                .UseNpgsql(connectionString, options =>
                {
                    options.EnableRetryOnFailure(3, TimeSpan.FromSeconds(30), null);
                }));

            return services;
        }
    }
}