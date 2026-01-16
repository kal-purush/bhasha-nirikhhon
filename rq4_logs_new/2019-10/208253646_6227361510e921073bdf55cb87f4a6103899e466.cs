using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace TheFipster.Munchkin.Configuration
{
    public static class AspCoreAppSettingsExtension
    {
        private const string AppSettingsFilename = "appsettings.json";

        public static void UseAppSettings(this IWebHostBuilder hostBuilder) =>
            hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.SetBasePath(Directory.GetCurrentDirectory());
                config.AddJsonFile(AppSettingsFilename);
            });
    }
}