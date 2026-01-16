using GiftSuggester.Core.Exceptions;
using GiftSuggester.Data;
using Microsoft.EntityFrameworkCore;

namespace GiftSuggester.Web.HostedServices;

public class MigrationHostedService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;

    public MigrationHostedService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();

        var context = scope.ServiceProvider.GetService<GiftSuggesterContext>() ??
                      throw new ServiceNotRegisteredException($"{nameof(GiftSuggesterContext)} is not registered!");

        await context.Database.MigrateAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}