using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using P2Project.SharedKernel;

namespace P2Project.Volunteers.Infrastructure.BackgroundServices;

public class DeleteExpiredSoftDeletedEntityBackgroundService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<DeleteExpiredSoftDeletedEntityBackgroundService> _logger;

    public DeleteExpiredSoftDeletedEntityBackgroundService(
        IServiceScopeFactory scopeFactory,
        ILogger<DeleteExpiredSoftDeletedEntityBackgroundService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("DeleteExpiredSoftDeletedEntityBackgroundService started");
        
        while (!cancellationToken.IsCancellationRequested)
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            
            var deleteExpiredSoftDeletedEntityService = scope.ServiceProvider
                .GetRequiredService<DeleteExpiredSoftDeletedEntityService>();

            _logger.LogInformation("DeleteExpiredSoftDeletedEntityService is working");

            await deleteExpiredSoftDeletedEntityService.Process(cancellationToken);

            await Task.Delay(
                TimeSpan.FromHours(Constants.DELETE_EXPIRED_SOFT_DELETED_SERVICE_DELAY_HOURS),
                cancellationToken);
        }
    }
}