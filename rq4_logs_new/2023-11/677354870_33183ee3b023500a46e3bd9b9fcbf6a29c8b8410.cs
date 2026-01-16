using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore;

namespace Mod4.Lection4.Hw.Context;

internal class EFCoreDbContextFactory : IDesignTimeDbContextFactory<EFCoreContext>
{
    private static string? _connectionString;

    public EFCoreContext CreateDbContext()
    {
        return CreateDbContext(null);
    }

    public EFCoreContext CreateDbContext(string[] args)
    {
        if (string.IsNullOrEmpty(_connectionString))
        {
            LoadConnectionString();
        }

        var builder = new DbContextOptionsBuilder<EFCoreContext>();  // вказуємо, який контекст використовуємо
        builder.UseSqlServer(_connectionString);  // який провайдер будемо використовувати і для якого провайдера буде згенерований код

        return new EFCoreContext(builder.Options);
    }

    private static void LoadConnectionString()
    {
        var builder = new ConfigurationBuilder();
        builder
            .AddJsonFile("appsettings.json", optional: false)
            .AddUserSecrets<EFCoreDbContextFactory>();

        var configuration = builder.Build();

        _connectionString = configuration.GetConnectionString(nameof(EFCoreContext));
    }
}