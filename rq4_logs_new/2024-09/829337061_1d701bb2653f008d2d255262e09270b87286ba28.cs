using WarehouseServiceAPI.Infrastructure;
using WarehouseServiceAPI.Models;

namespace WarehouseService.FunctionalTests.Setup
{
    public class TestingScenarioBase
    {
        protected static HttpClient GetClient()
        {
            var factory = new WarehouseServiceFactory();
            var scope = factory.Services.CreateScope();
            SeedDatabase(scope);
            return factory.CreateClient();
        }

        static void SeedDatabase(IServiceScope scope)
        {
            var context = scope.ServiceProvider.GetRequiredService<WarehouseContext>();
            context.Database.EnsureCreated();

            Part part = new("Test part", "Test description", "Test Manufacturer", "Test Model", 10, 5);
            context.Parts.Add(part);
            context.SaveChanges();
        }
    }
}