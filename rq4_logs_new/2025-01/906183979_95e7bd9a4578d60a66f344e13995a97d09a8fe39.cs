using Application.Interfaces.BlobStorageInterface;
using Infrastructure.Services.BlobStorageService;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Infrastructure
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration) 
        {
            services.AddSingleton<IBlobStorage, BlobStorageService>();

            return services;
        }
    }
}