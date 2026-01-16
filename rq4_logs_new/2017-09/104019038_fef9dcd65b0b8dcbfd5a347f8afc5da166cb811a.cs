using Microsoft.AspNetCore.Hosting;
using System;
using System.IO;

namespace TheBarback
{
    public class Program
    {
        static void Main(string[] args)
        {
            var host =
                new WebHostBuilder().UseKestrel()
                    .UseContentRoot(Directory.GetCurrentDirectory())
                    .UseIISIntegration()
                    .UseStartup<Startup>()
                    .CaptureStartupErrors(false)
                    .Build();

            host.Run();
        }
    }
}