using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;

namespace Goal.Infrastructure.Persistence;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<GoalDatabaseContext>
{
    public GoalDatabaseContext CreateDbContext(string[] args)
    {
        // Určuje adresář, kde je umístěn appsettings.Development.json
        var currentDirectory = Directory.GetCurrentDirectory();
        var apiProjectPath = Path.Combine(currentDirectory, "..", "Goal.Api");

        // Nastavení prostředí
        var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Development";

        // Načítání konfigurace
        var configuration = new ConfigurationBuilder()
            .SetBasePath(apiProjectPath) // Explicitní cesta k projektu Goal.Api
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true)
            .Build();

        // Načtení connection stringu
        var connectionString = configuration.GetConnectionString("SqlServerConnection");
        Console.WriteLine($"ASPNETCORE_ENVIRONMENT: {environment}");
        Console.WriteLine($"API Project Path: {apiProjectPath}");
        Console.WriteLine($"Connection String: {connectionString}");
        // Konfigurace DbContextu
        var optionsBuilder = new DbContextOptionsBuilder<GoalDatabaseContext>();
        optionsBuilder.UseSqlServer(connectionString);

        return new GoalDatabaseContext(optionsBuilder.Options);
    }
}