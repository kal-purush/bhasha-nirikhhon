using Microsoft.Extensions.DependencyInjection;
using StudyingMvcCore.Business.Interfaces;
using StudyingMvcCore.Data.Repository;

namespace StudyingMvcCore.App.Configurations
{
    public static class DependencyInjectionConfig
    {
        public static IServiceCollection ResolveDependencies(this IServiceCollection services)
        {
            services.AddScoped<ICustomerRepository, CustomerRepository>();
            services.AddScoped<IToDoRepository, ToDoRepository>();
            services.AddScoped<IAddressRepository, AddressRepository>();

            return services;
        }
    }
}