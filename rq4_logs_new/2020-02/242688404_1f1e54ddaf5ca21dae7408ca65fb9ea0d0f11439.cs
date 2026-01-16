using Microsoft.Extensions.DependencyInjection;

namespace SalesTaxes
{
    public sealed class ServiceProviderSingleton
    {
        private static ServiceProviderSingleton instance = null;
        private static readonly object padlock = new object();
        private ServiceProvider _serviceProvider;
        
        public ServiceProvider ServiceProvider { get => _serviceProvider; }

        ServiceProviderSingleton()
        {
             _serviceProvider = new ServiceCollection()
            .AddTransient<ITaxCalculator, TaxCalculator>()
            .BuildServiceProvider();
        }

        public static ServiceProviderSingleton Instance
        {
            get
            {
                lock (padlock)
                {
                    if (instance == null)
                    {
                        instance = new ServiceProviderSingleton();
                    }
                    return instance;
                }
            }
        }
    }
}