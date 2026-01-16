using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Weikio.ApiFramework.Plugins.Epassi.Sample;

namespace Weikio.ApiFramework.Plugins.Browser.Sample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}