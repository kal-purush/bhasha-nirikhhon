using System.Threading.Tasks;
using AwakenServer.Activity.Dtos;
using MassTransit;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Volo.Abp.DependencyInjection;

namespace AwakenServer.Hubs;

public class ActivityRankingListHandler : IConsumer<NewIndexEvent<RankingListDto>>, ITransientDependency
{
    private readonly IHubContext<TradeHub> _hubContext;
    private readonly ITradeHubGroupProvider _tradeHubGroupProvider;
    private readonly ILogger<ActivityRankingListHandler> _logger;

    public ActivityRankingListHandler(IHubContext<TradeHub> hubContext, ITradeHubGroupProvider tradeHubGroupProvider, ILogger<ActivityRankingListHandler> logger)
    {
        _hubContext = hubContext;
        _tradeHubGroupProvider = tradeHubGroupProvider;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<NewIndexEvent<RankingListDto>> context)
    {
        var activityRankingListGroup =
            _tradeHubGroupProvider.GetActivityRankingListGroupName(context.Message.Data.ActivityId);
        _logger.LogInformation(
            "ActivityRankingListHandler: Consume RankingListDto:activityRankingListGroup:ActivityId:{ActivityId}",
            context.Message.Data.ActivityId);
        await _hubContext.Clients.Group(activityRankingListGroup).SendAsync("ReceiveActivityRankingList", context.Message.Data);
    }
}