using Microsoft.Extensions.DependencyInjection;
using Splat;
using Splat.Microsoft.Extensions.DependencyInjection;

namespace Netch.Services
{
    public static class DI
    {
        public static T GetService<T>()
        {
            return Locator.Current.GetService<T>();
        }

        public static void Register()
        {
            var services = new ServiceCollection();

            services.UseMicrosoftDependencyResolver();
            Locator.CurrentMutable.InitializeSplat();

            ConfigureServices(services);
        }

        private static IServiceCollection ConfigureServices(IServiceCollection services)
        {
            services.AddViews();
            services.AddConfig();
            services.AddDynamicData();
            return services;
        }
    }
}