using System;

namespace home_swap_api.Helpers
{
	public class UserCleanupService : BackgroundService
	{
        private readonly ILogger<UserCleanupService> _logger;

        public UserCleanupService(ILogger<UserCleanupService> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                
                await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

                _logger.LogInformation("Service called");

                
            }
        }
    }
}
