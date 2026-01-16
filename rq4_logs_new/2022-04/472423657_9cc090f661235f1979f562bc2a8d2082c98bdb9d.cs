using Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Logic.HostedServices
{
    public class TokenCleanerHostedService : IHostedService, IDisposable
    {
        private readonly ILogger<TokenCleanerHostedService> logger;
        private readonly IServiceScopeFactory scopeFactory;
        private Timer timer = null!;

        public TokenCleanerHostedService(ILogger<TokenCleanerHostedService> logger, IServiceScopeFactory scopeFactory)
        {
            this.logger = logger;
            this.scopeFactory = scopeFactory;
        }

        public Task StartAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Token Cleaner Hosted Service running.");

            timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromMinutes(30));
            
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Token Cleaner Hosted Service is stopping.");

            timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            timer?.Dispose();
        }

        private void DoWork(object? state)
        {
            using (var scope = scopeFactory.CreateScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<QuettaDbContext>();
                
                var tokens = dbContext.RefreshTokens.Where(t => t.Expires < DateTime.UtcNow);
                var count = tokens.Count();
                dbContext.RemoveRange(tokens);
                dbContext.SaveChanges();

                logger.LogInformation("Token Cleaner Hosted Service is working. Removed tokens: {Count}", count);
            }
            
        }
    }
}