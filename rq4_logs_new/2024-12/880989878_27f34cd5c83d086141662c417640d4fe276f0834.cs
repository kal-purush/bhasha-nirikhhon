using MarGate.Core.Persistence.Context;
using MarGate.Core.Persistence.UnitOfWork;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MarGate.Core.Persistence.Extension;
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddUnitOfWork<TWriteContext>(this IServiceCollection services, IConfiguration configuration) where TWriteContext : WriteDbContext
    {
        services.AddScoped<IUnitOfWork, UnitOfWork.UnitOfWork>();

        services.AddScoped((_) =>
        {
            var optionsBuilder = new DbContextOptionsBuilder<WriteDbContext>();
            optionsBuilder.UseSqlServer(configuration.GetConnectionString("WriteDb"));
            return new WriteDbContext(optionsBuilder.Options);

        });

        return services;
    }

    public static IServiceCollection AddUnitOfWork<TWriteContext, TReadContext>(this IServiceCollection services, IConfiguration configuration) where TWriteContext : WriteDbContext where TReadContext : ReadDbContext
    {
        services.AddScoped<IUnitOfWork, UnitOfWork.UnitOfWork>();
        services.AddScoped((_) =>
        {
            var optionsBuilder = new DbContextOptionsBuilder<WriteDbContext>();
            optionsBuilder.UseSqlServer(configuration.GetConnectionString("WriteDb"));
            return new WriteDbContext(optionsBuilder.Options);

        });

        services.AddScoped((_) =>
        {
            var optionsBuilder = new DbContextOptionsBuilder<ReadDbContext>();
            optionsBuilder.UseSqlServer(configuration.GetConnectionString("ReadDb"));
            return new ReadDbContext(optionsBuilder.Options);

        });

        return services;
    }
}