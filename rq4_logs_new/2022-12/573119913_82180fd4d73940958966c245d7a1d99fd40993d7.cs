using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using VanKassa.Backend.Infrastructure.Data;
using VanKassa.Domain;

namespace VanKassa.Backend.Core.Data;

public static class DbContextProvider
{
    public static IServiceCollection ConfigureDatabase(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddDbContext<VanKassaDbContext>(x => x.UseNpgsql(
            configuration.GetConnectionString(SettingConstant.PostgresDatabase),
            y => y.MigrationsAssembly(typeof(VanKassaDbContext).Assembly.FullName)));

        return services;
    }
}