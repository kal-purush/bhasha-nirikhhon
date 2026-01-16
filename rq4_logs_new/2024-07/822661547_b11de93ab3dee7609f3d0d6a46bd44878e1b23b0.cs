using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ShortSharing.DAL.DI;
using ShortSharing.DAL.Interceptors;

namespace ShortSharing.BLL.DI
{
    public static class DependencyInjection
    {
        public static void AddBusinessLogicDependencies(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddScoped<AuditableEntitiesInterceptor>();

            services.AddDataAccessDependencies(configuration);
        }
    }
}