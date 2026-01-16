using DS.DataAccess.Database;
using DS.Infrastructure.Dependency;
using Microsoft.Extensions.DependencyInjection;

namespace DS.DataAccess
{
    public static class DataAccessDiConfig
    {
        public static void RegisterRepositories(IServiceCollection services)
        {
            DependencyRegistration.RegisterScopedInterfaces(services, typeof(DataAccessDiConfig), @namespace: "DS.DataAccess.Repositories", componentNameSuffix: "Repository");

            services.AddScoped<IDbContext>(provider => new InMemoryDatabase());
        }
    }
}