
using WolfDen.Application.Constants;
using WolfDen.Application.Models;

namespace WolfDen.API.BackgroudWorkers
{
    public class DatabaseSyncBackgroundService : BackgroundService
    {
        private readonly ILogger<DatabaseSyncBackgroundService> _logger;
        private readonly QueryBasedSyncService _syncService;
        private readonly Dictionary<string, TableSyncConfig> _syncConfigs;
        private readonly int _intervalInMinutes;
        public DatabaseSyncBackgroundService(
       ILogger<DatabaseSyncBackgroundService> logger,
       IConfiguration configuration)
        {
            _logger = logger;
            _intervalInMinutes = configuration.GetValue<int>("SyncSettings:IntervalInMinutes", 1);

            var sourceConn = configuration.GetConnectionString("BioMetricDatabase");
            var destConn = configuration.GetConnectionString("DatabaseConnection");
            _syncService = new QueryBasedSyncService(sourceConn, destConn);

            _syncConfigs = SyncConfigs.InitializeSyncConfigs();
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Database sync started at: {time}", DateTimeOffset.Now);
                    await _syncService.SyncTablesAsync(_syncConfigs);
                    _logger.LogInformation("Database sync completed at: {time}", DateTimeOffset.Now);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred while syncing databases");
                }

                await Task.Delay(TimeSpan.FromMinutes(_intervalInMinutes), stoppingToken);
            }
        }


      
    }
}