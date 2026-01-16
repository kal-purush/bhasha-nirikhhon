using Microsoft.Extensions.DependencyInjection;
using OnlineStore.Domain.Entities;
using OnlineStore.Infrastructure.Interfeces;
using OnlineStore.Infrastructure.Respositories;

namespace OnlineStore.Infrastructure.Extensions
{
    public static class ExtensionsService
    {
        public static void AddInfrastructureService(this IServiceCollection services)
        {
            services.AddScoped<IUnitOfWork, UnitOfWork>();
            services.AddDbContext<OnlinestoreContext>();
        }
    }
}