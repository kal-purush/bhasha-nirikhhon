using Taskly.Dashboard.App.Repositories;
using Taskly.Dashboard.App.Repositories.Interfaces;
using Taskly.Dashboard.App.Services;
using Taskly.Dashboard.App.Services.Interfaces;

namespace Taskly.Dashboard.App
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddUserServices(this IServiceCollection services)
        {
            services.AddScoped<IUserRepository, UserRepository>();
            services.AddScoped<IUserService, UserService>();

            return services;
        }
    }
}