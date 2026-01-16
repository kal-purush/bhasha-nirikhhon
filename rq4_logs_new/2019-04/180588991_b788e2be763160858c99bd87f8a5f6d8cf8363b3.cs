using System;
using System.Threading;
using System.Threading.Tasks;
using Catalog.BusinessLogicLayer.Service;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Catalog.Daemon
{
    public class OperationalDataCollectorService : IHostedService, IDisposable
    {
        private readonly object _lockObject = new object();
        private readonly ILogger _logger;
        private readonly XRoadOperationalDataCollector _collector;
        private Timer _timer;

        public OperationalDataCollectorService(ILogger<OperationalDataCollectorService> logger
            , XRoadOperationalDataCollector collector)
        {
            _logger = logger;
            _collector = collector;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OperationalDataCollectorService) + " is starting");

            _timer = new Timer(Collect, null, TimeSpan.Zero, TimeSpan.FromHours(2));

            return Task.CompletedTask;
        }

        private void Collect(object state)
        {
            lock (_lockObject)
            {
                _logger.LogInformation("Starting Operational Data Collector Task");
                _collector.RunOpDataCollectorTask();
                _logger.LogInformation("Operational Data Collector Task Completed");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(OperationalDataCollectorService) + " is stopping");

            _timer?.Change(Timeout.Infinite, 0);
            
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}