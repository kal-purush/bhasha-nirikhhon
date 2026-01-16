using Microsoft.Extensions.Configuration;
using RuntimeConfig.SqlServer;
using System;
using System.IO;
using System.Threading;

namespace RuntimeConfig.ConsoleSample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(@"appsettings.json");

            var settings = builder.Build();
            builder.AddSqlServerDatabase(settings["Data:DefaultConnection:ConnectionString"], settings["Data:DefaultConnection:DatabaseName"], pollingIntervalSeconds: 3);
            settings = builder.Build();

            while (true)
            {
                var value = settings["MySetting"];

                Console.WriteLine($"MySetting = '{value}'");
                Thread.Sleep(2000);
            }
        }
    }
}