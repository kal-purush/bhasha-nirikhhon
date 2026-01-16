using Microsoft.EntityFrameworkCore;
using Serilog;

namespace AirQuality.Core.DAL;

public class DbInitializer<T>(IDbContextFactory<T> factory)
    where T : DbContext
{
    public async Task InitAsync()
    {
        try
        {
            await using var dbContext = await factory.CreateDbContextAsync();

            await dbContext.Database.MigrateAsync();
            
            Log.Information("Миграции успешно применены");
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка при выполнении миграций - " + ex.Message);
        }
    }
}