using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using SwiftConductor.Client.Interfaces;
using SwiftConductor.Client.Extensions;

namespace SwiftConductor.Client.Worker
{
    public class WorkerHost
    {
        public static IHost Create(LogLevel logLevel = LogLevel.Information)
        {
            return Create(Conductor.GetConfiguration(), logLevel);
        }

        public static IHost Create(Configuration configuration, LogLevel logLevel = LogLevel.Information)
        {
            return Create(configuration, logLevel, workers: new IWorker[0]);
        }

        public static IHost Create<T>(LogLevel logLevel = LogLevel.Information, 
                                                params T[] workers) where T : IWorker
        {
            return Create(Conductor.GetConfiguration(), logLevel, workers);
        }

        public static IHost Create<T>(Configuration configuration, 
                                                LogLevel logLevel = LogLevel.Information, 
                                                params T[] workers) where T : IWorker
        {
            var hostBuilder = new HostBuilder().ConfigureServices((ctx, services) => {
                services.AddWorkerManager(configuration);
                foreach (var worker in workers) {
                    services.AddWorker(worker);
                }
             
                services.WithHostedService();
            });

            hostBuilder.ConfigureLogging(logging => {
                logging.SetMinimumLevel(logLevel);
                logging.AddConsole();
            });

            return hostBuilder.Build();
        }
    }
}