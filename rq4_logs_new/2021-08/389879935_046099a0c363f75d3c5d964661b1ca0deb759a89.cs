using Microsoft.Extensions.DependencyInjection;

namespace CarsInfo.Infrastructure.DependencyInjection
{
    public static class DependencyInjection
    {
        public static void AddInfrastructure(
            this IServiceCollection services,
            string masterConnectionString,
            string carsInfoConnectionString)
        {
            services.AddDbInitialization(masterConnectionString, carsInfoConnectionString);
            services.AddPersistenceLayer(carsInfoConnectionString);
            services.AddBusinessLogicLayer();
        }
    }
}