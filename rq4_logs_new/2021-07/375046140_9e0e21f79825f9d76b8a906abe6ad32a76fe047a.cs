using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Infrastructure.Data;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Api
{
    public class Program
    {
        public static async Task Main( string[] args )
        {
            var host = CreateHostBuilder ( args ).Build();

            using ( var scope = host.Services.CreateScope() )
            {
                var services = scope.ServiceProvider;
                
                // Collect log information
                var loggerFactory = services.GetRequiredService<ILoggerFactory>();

                // When application is starting, then create database from existing migration
                try
                {
                    var context = services.GetRequiredService<HalkoContext>();
                    await context.Database.MigrateAsync();
                }
                // Something was wrong like migration file not exist, missing nugget package, ...
                catch ( Exception ex )
                {
                    var logger = loggerFactory.CreateLogger<Program>();
                    logger.LogError ( ex, "An error occured during migration" );
                }
                
                // Run Application if all is fine
                await host.RunAsync ();
            }
        }

        public static IHostBuilder CreateHostBuilder( string[] args ) =>
            Host.CreateDefaultBuilder ( args )
                .ConfigureWebHostDefaults ( webBuilder => { webBuilder.UseStartup<Startup>(); } );
    }
}