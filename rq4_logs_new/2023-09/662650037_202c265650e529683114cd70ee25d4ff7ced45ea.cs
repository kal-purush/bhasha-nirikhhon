using Microsoft.AspNetCore.Authorization;
using Shared.Permission;
using UI.Managers;

namespace UI.Configuration
{
    public static class ServiceConfiguration
    {
        internal static IServiceCollection AddCustomServices(this IServiceCollection services, IConfiguration _configuration)
        {
            //Transient objects are always different; a new instance is provided to every controller and every service.
            //Scoped objects are the same within a request, but different across different requests.
            //Singleton objects are the same for every object and every request.

            services.AddSingleton<IAuthorizationPolicyProvider, PermissionPolicyProvider>();
            services.AddScoped<IAuthorizationHandler, PermissionAuthorizationHandler>();
            services.AddHttpContextAccessor();
            services.AddManagers();
           
            //services.AddSingleton<GlobalVariable>();


            return services;
        }
        public static IServiceCollection AddManagers(this IServiceCollection services)
        {
            var managers = typeof(IManager);

            var types = managers
                .Assembly
                .GetExportedTypes()
                .Where(t => t.IsClass && !t.IsAbstract)
                .Select(t => new
                {
                    Service = t.GetInterface($"I{t.Name}"),
                    Implementation = t
                })
                .Where(t => t.Service != null);

            foreach (var type in types)
            {
                if (managers.IsAssignableFrom(type.Service))
                {
                    services.AddTransient(type.Service, type.Implementation);
                }
            }

            return services;
        }

    }
}