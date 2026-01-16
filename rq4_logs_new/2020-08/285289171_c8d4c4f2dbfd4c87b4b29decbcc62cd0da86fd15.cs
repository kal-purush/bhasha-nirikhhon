using Microsoft.EntityFrameworkCore;

using System;
using System.Data;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddEfRepository<TDbContext>(this IServiceCollection services, Action<DbContextOptionsBuilder> options)
            where TDbContext : DbContext, IEfRepositoryDbContext
        {
            services.AddScoped<IEfRepositoryDbContext, TDbContext>();
            services.AddDbContext<TDbContext>(options, ServiceLifetime.Scoped);
            services.AddScoped(typeof(IRepository<>), typeof(EntityFrameworkRepository<>));
            return services;
        }
    }
}