using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace TheFipster.Munchkin.Configuration
{
    public static class Extensions
    {
        public static List<string> GetArray(this IConfiguration config, string key) => config
            .GetSection(key)
            .AsEnumerable()
            .Where(x => !string.IsNullOrWhiteSpace(x.Value))
            .Select(x => x.Value)
            .ToList();

        public static IWebHostBuilder UseAppSettings(this IWebHostBuilder hostBuilder)
        {
            hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.SetBasePath(Directory.GetCurrentDirectory());
                config.AddJsonFile("appsettings.json");
            });
            return hostBuilder;
        }
    }
}