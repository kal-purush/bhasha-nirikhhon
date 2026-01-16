using System;
using System.Threading;
using System.Threading.Tasks;
using Catalog.BusinessLogicLayer.Service;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Catalog.Daemon.HostedService
{
    public class XRoadMetadataCollectorService : IHostedService, IDisposable
    {
        private readonly object _lockObject = new object();
        private readonly ILogger _logger;
        private readonly XRoadMetadataCollector _collector;
        private Timer _timer;

        public XRoadMetadataCollectorService(
            XRoadMetadataCollector collector
            , ILogger<XRoadMetadataCollector> logger)
        {
            _collector = collector;
            _logger = logger;
        }


        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(XRoadMetadataCollectorService) + " is starting");
            _timer = new Timer(Collect, null, TimeSpan.Zero, TimeSpan.FromHours(1));
            return Task.CompletedTask;
        }

        private void Collect(object state)
        {
            lock (_lockObject)
            {
                _logger.LogInformation("Starting XRoad Metadata Collector Task");
                var task = Task.Run(async () => { await _collector.RunBatchUpdateTask(); });
                task.Wait();
                _logger.LogInformation("XRoad Metadata Collector Task Completed");
            }
        }


        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation(nameof(XRoadMetadataCollectorService) + " is stopping");

            _timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}