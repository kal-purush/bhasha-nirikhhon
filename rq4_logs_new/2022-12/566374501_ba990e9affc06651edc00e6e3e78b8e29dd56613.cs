using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Standard.Tool.Platform.Data;
using Standard.Tool.Platform.PostgreSql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Standard.Tool.Platform;

public static class ApplicationExtensions
{
    public static async Task<StartupInitResult> InitStartUp(this IHost app)
    {
        var services = app.Services;

        var context = services.GetRequiredService<PostgreSqlToolsBlockDbContext>();
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
        });

        var logger = loggerFactory.CreateLogger<App>();
        bool canConnect = await context.Database.CanConnectAsync();
        if (!canConnect) return StartupInitResult.DatabaseConnectionFail;

        await context.Database.EnsureCreatedAsync();


        bool isNew = !await context.Account.AnyAsync();
        if (isNew)
        {
            try
            {
                logger.LogInformation("Seeding database...");

                await context.ClearAllData();
                await Seed.SeedAsync(context, logger);

                logger.LogInformation("Database seeding successfully.");

            }
            catch (Exception e)
            {
                logger.LogCritical(e, e.Message);
                return StartupInitResult.DatabaseSetupFail;
            }
        }

        return StartupInitResult.None;
    }
}

public enum StartupInitResult
{
    None = 0,
    DatabaseConnectionFail = 1,
    DatabaseSetupFail = 2
}