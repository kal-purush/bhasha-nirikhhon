using Blazored.LocalStorage;
using DeusVultClicker.Client.Buildings.Store.Selectors;
using DeusVultClicker.Client.Eras.Store.Selectors;
using DeusVultClicker.Client.Shared;
using DeusVultClicker.Client.Shared.Store.Selectors;
using DeusVultClicker.Client.Upgrades.Store.Selector;
using Fluxor;
using Microsoft.Extensions.DependencyInjection;

namespace DeusVultClicker.Client.DependecyInjection
{
    public static class DeusVultClickerExtensions
    {
        public static IServiceCollection AddDeusVultClicker(this IServiceCollection serviceCollection, bool useDevTools)
        {
            serviceCollection.AddBlazoredLocalStorage(config => config.JsonSerializerOptions.WriteIndented = true);

            serviceCollection.AddTransient<AvailableUpgradesSelector>();
            serviceCollection.AddTransient<AvailableBuildingsSelector>();
            serviceCollection.AddTransient<OwnedAdvancementsSelector>();
            serviceCollection.AddTransient<AvailableEraSelector>();
            serviceCollection.AddTransient<UpgradeEffectsSelector>();
            serviceCollection.AddScoped<TimerService>();

            serviceCollection.AddFluxor(options =>
            {
                if (useDevTools)
                    options.ScanAssemblies(typeof(Program).Assembly).UseReduxDevTools();
                else
                    options.ScanAssemblies(typeof(Program).Assembly);
            });

            return serviceCollection;
        }
    }
}