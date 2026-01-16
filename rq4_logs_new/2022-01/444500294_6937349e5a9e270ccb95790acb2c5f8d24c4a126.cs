using Microsoft.EntityFrameworkCore;
using PlatformService.Models;

namespace PlatformService.Data;

public static class DataSeeder
{
    public static void SeedSampleData(this IApplicationBuilder app, bool isProduction)
    {
        using var serviceScope = app.ApplicationServices.CreateScope();
        SeedData(serviceScope.ServiceProvider.GetService<AppDbContext>(), isProduction);
    }

    private static void SeedData(AppDbContext? context, bool isProduction)
    {
        try
        {
            if (isProduction && context is not null)
            {
                Console.WriteLine("--> Applying migrations in Production");
                context.Database.Migrate();
            }
        }
        catch (Exception exception)
        {
            Console.WriteLine($"--> Applying migrations went wrong: {exception.Message}");
        }

        if (context?.Platforms != null && !context.Platforms.Any())
        {
            Console.WriteLine("--> Seeding Data...");
            context.Platforms.AddRange(
                new Platform { Name = "Dot Net", Publisher = "Microsoft", Cost = "Free" },
                new Platform { Name = "SQL Server Express", Publisher = "Microsoft", Cost = "Free" },
                new Platform { Name = "Kubernetes", Publisher = "Cloud Native Computing Foundation", Cost = "Free" }
            );
            context.SaveChanges();
        }
        else
        {
            Console.WriteLine("--> We already have data");
        }
    }
}