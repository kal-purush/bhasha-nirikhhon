using MeowLib.DAL;
using Microsoft.EntityFrameworkCore;

namespace MeowLib.WebApi.Extensions;

public static class AppBuilderExtensions
{
    public static void ApplyMigrations(this IApplicationBuilder app)
    {
        using var serviceScope = app.ApplicationServices.CreateScope();
        var logService = serviceScope.ServiceProvider.GetRequiredService<ILogger<ApplicationDbContext>>();
        using var dbContext = serviceScope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
        logService.LogInformation("Начало применения миграций");
        dbContext.Database.Migrate();
        logService.LogInformation("Миграции успешно применены");
    }
}