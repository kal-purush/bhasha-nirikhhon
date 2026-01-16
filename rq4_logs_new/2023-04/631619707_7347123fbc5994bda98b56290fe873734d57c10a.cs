using Microsoft.EntityFrameworkCore;
using PocketForzaHorizonCommunity.Back.Database;

namespace PocketForzaHorizonCommunity.Back.API.ServiceConfig;

public static class DevelopmentEnviromentSeederConfig
{
    public static IServiceScope RunDevelopmentEnvironmentSeeder(this IServiceScope scope)
    {
        var context = scope.ServiceProvider.GetService<ApplicationDbContext>();

        using (var connection = context.Database.GetDbConnection())
        {
            connection.Open();

            var seeder = new DevelopmentEnvironmentSeeder(scope.ServiceProvider);
            seeder.Seed().Wait();
        }

        return scope;
    }
}