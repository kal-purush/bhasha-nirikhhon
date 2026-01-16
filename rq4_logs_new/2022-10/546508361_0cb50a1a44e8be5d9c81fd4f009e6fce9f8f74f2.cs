using FridgeApi.Interfaces;
using FridgeApi.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Persistence;

namespace FridgeApi.Extensions
{
    public static class ServiceExtentions
    {
        public static void ConfigureCors(this IServiceCollection services)
        {
            services.AddCors(options =>
            {
                options.AddDefaultPolicy(builder =>
                    builder.AllowAnyOrigin()
                        .AllowAnyMethod()
                        .AllowAnyHeader());
            });
        }
        public static void ConfigureLoggerService(this IServiceCollection services) =>
services.AddScoped<ILoggerManager, LoggerManager>();
        public static IServiceCollection ConfigureSqlContext(this IServiceCollection services,
   IConfiguration configuration)
        {
            var connectionString = configuration.GetConnectionString("DbConnection");
            services.AddDbContext<FridgeDbContext>(options =>
            {
                options.UseSqlServer(connectionString, migration =>
                        migration.MigrationsAssembly("Persistence"));
            });
            return services;
        }
    }
}