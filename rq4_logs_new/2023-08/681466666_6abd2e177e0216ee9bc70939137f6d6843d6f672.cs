using BetterCoding.MessagePubSubCenter.Repository.ElasticSearch;
using BetterCoding.MessagePubSubCenter.Services.Pipelines.Webhook;
using BetterCoding.Patterns.Pipeline;

namespace BetterCoding.MessagePubSubCenter.Services.Pipelines
{
    public class AuditLog: AutomicTransactionPipeline<WebhookPayloadContext>
    {
        IElasticSearchRepository _elasticSearchRepository;
        public AuditLog(IElasticSearchRepository elasticSearchRepository)
        {
            _elasticSearchRepository = elasticSearchRepository;
        }

        public override async Task<WebhookPayloadContext> ProcessAsync(WebhookPayloadContext input)
        {
            var entryLog = new EntryLog(input);
            await _elasticSearchRepository.AddAsync(input.Payload, entryLog.CollectionName);
            return input;
        }

        public override Task<WebhookPayloadContext> RevertAsync(WebhookPayloadContext input)
        {
            throw new NotImplementedException();
        }
    }
}