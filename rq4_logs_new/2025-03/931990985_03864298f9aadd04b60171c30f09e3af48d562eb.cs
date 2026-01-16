using Microsoft.Extensions.DependencyInjection;
using OrderService.Application.Utilities;
using System.Reflection;

namespace OrderService.Application
{
    public static class DependencyInjection
    {
        public static void ConfigureApplicationServices(this IServiceCollection services)
        {
            services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(DependencyInjection).Assembly));

            services.AddAutoMapper(Assembly.GetExecutingAssembly());

            services.AddScoped<OrderNumberGenerator>();
        }
    }
}