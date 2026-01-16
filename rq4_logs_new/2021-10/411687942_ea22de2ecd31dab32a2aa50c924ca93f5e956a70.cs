using System.IO;
using Microsoft.Extensions.Configuration;

namespace Persistence
{
    public static class AppConfig
    {
        public static string GetConnectionString()
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile(Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json"), optional: false)
                .AddJsonFile(Path.Combine(Directory.GetCurrentDirectory(), "appsettings.Development.json"), optional: true)
                .Build();
            var d = configuration.GetSection("ConnectionStrings").GetSection("ConString").Value;
            return d;
        }
    }
}