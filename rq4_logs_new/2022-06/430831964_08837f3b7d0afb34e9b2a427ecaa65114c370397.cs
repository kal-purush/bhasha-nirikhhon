using Integration.Common.Services.StocksAndPrices;

namespace Api.BackgroundServices.Marketplaces;

public class TrackAllStocksAndPricesBackgroundService : BackgroundService
{
    private readonly bool _active;
    
    public TrackAllStocksAndPricesBackgroundService(ILogger<TrackAllStocksAndPricesBackgroundService> logger, IServiceProvider services,
        IConfiguration configuration) : base(logger, services)
    {
        TimerInterval = configuration.GetValue<int>("MarketplaceSettings:StocksAndPricesSynchronizationJob:ExecutionInterval");
        _active = configuration.GetValue<bool>("MarketplaceSettings:StocksAndPricesSynchronizationJob:Active");
    }

    protected override async Task ServiceWork()
    {
        if (!_active)
        {
            return;
        }
        
        await using AsyncServiceScope scope = Services.CreateAsyncScope();
        var taskHandler = scope.ServiceProvider.GetRequiredService<ITrackAllStocksAndPricesService>();

        await taskHandler.TrackAllStocksAndPrices();
    }
}