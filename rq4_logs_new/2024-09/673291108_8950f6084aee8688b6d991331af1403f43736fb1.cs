using System;
using System.Threading.Tasks;
using AElf.Indexing.Elasticsearch;
using AwakenServer.Activity.Eto;
using AwakenServer.Activity.Index;
using AwakenServer.EntityHandler.Trade;
using Microsoft.Extensions.Logging;
using Volo.Abp.EventBus.Distributed;

namespace AwakenServer.EntityHandler.Activity;

public class JoinRecordIndexHandler : TradeIndexHandlerBase, IDistributedEventHandler<JoinRecordEto>
{
    private readonly INESTRepository<JoinRecordIndex, Guid> _joinRecordIndexRepository;
    private readonly ILogger<JoinRecordIndexHandler> _logger;

    public JoinRecordIndexHandler(INESTRepository<JoinRecordIndex, Guid> joinRecordIndexRepository, ILogger<JoinRecordIndexHandler> logger)
    {
        _joinRecordIndexRepository = joinRecordIndexRepository;
        _logger = logger;
    }

    public async Task HandleEventAsync(JoinRecordEto eventData)
    {
        var joinRecordIndex = ObjectMapper.Map<JoinRecordEto, JoinRecordIndex>(eventData);
        var existedIndex = await _joinRecordIndexRepository.GetAsync(q =>
            q.Term(i => i.Field(f => f.ActivityId).Value(eventData.ActivityId)) &&
            q.Term(i => i.Field(f => f.Address).Value(eventData.Address)));
        joinRecordIndex.Id = existedIndex switch
        {
            null => Guid.NewGuid(),
            _ => existedIndex.Id
        };
        await _joinRecordIndexRepository.AddOrUpdateAsync(joinRecordIndex);
    }
}