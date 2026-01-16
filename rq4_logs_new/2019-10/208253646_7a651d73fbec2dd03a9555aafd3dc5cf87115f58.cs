using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using System.IO;

namespace TheFipster.Munchkin.SampleApi
{
    public class Program
    {
        public static void Main(string[] args) => WebHost
            .CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((hostingContext, config) =>
                config
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables()
                    .AddCommandLine(args))
            .UseSerilog((hostingContext, loggerConfiguration) =>
                loggerConfiguration
                    .ReadFrom
                    .Configuration(hostingContext.Configuration))
            .UseStartup<Startup>()
            .Build()
            .Run();
    }
}