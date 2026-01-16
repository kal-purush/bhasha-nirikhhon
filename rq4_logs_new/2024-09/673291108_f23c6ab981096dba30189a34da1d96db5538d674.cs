using System;
using System.Threading.Tasks;
using AElf.Indexing.Elasticsearch;
using AwakenServer.EntityHandler.Trade;
using AwakenServer.StatInfo.Etos;
using AwakenServer.StatInfo.Index;
using MassTransit;
using Microsoft.Extensions.Logging;
using Volo.Abp.EventBus.Distributed;

namespace AwakenServer.EntityHandler.StatInfo;

public class PoolStatInfoIndexHandler : TradeIndexHandlerBase, IDistributedEventHandler<PoolStatInfoEto>
{
    private readonly INESTRepository<PoolStatInfoIndex, Guid> _repository;
    private readonly ILogger<PoolStatInfoIndexHandler> _logger;
    private readonly IBus _bus;

    public PoolStatInfoIndexHandler(INESTRepository<PoolStatInfoIndex, Guid> repository, ILogger<PoolStatInfoIndexHandler> logger, IBus bus)
    {
        _repository = repository;
        _logger = logger;
        _bus = bus;
    }

    public async Task HandleEventAsync(PoolStatInfoEto eventData)
    {
        var poolStatInfoIndex = ObjectMapper.Map<PoolStatInfoEto, PoolStatInfoIndex>(eventData);
        var existedIndex = await _repository.GetAsync(q =>
            q.Term(i => i.Field(f => f.ChainId).Value(eventData.ChainId)) &&
            q.Term(i => i.Field(f => f.Version).Value(eventData.Version)) &&
            q.Term(i => i.Field(f => f.PairAddress).Value(eventData.PairAddress)));
        poolStatInfoIndex.Id = existedIndex switch
        {
            null => Guid.NewGuid(),
            _ => existedIndex.Id
        };
        await _repository.AddOrUpdateAsync(poolStatInfoIndex);
    }
}