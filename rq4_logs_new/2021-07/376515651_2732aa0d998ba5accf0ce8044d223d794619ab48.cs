using CarService.AppCore.Services;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using MediatR;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using CarService.AppCore.Interfaces;
using CarService.Infrastructure.MongoDb.Repositories;
using CarService.Infrastructure.MongoDb;
using CarService.AppCore.Cqrs.Commands;
using System.Threading.Tasks;
using System.Reflection;
using CarService.AppCore.Cqrs.Commands.Handlers;
using CarService.Domain.Models;

namespace CarService.RepairOrders.Messenger
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();

            _ = host.Services.GetRequiredService<IEventListener>();

            host.Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureServices((appContext, services) =>
                    services
                        .AddMongoDb(appContext.Configuration)
                        .AddMediatR(typeof(CreateRepairOrderCommandHandler))
                        .AddSingleton<IEventListener, RedisEventListener>());
                        
        }
    }
}