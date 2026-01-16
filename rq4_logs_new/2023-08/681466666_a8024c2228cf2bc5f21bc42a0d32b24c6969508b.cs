using BetterCoding.MessagePubSubCenter.Repository.ElasticSearch;
using BetterCoding.MessagePubSubCenter.Services.Pipelines.Webhook;
using BetterCoding.Patterns.Pipeline;

namespace BetterCoding.MessagePubSubCenter.Services.Pipelines
{
    public class EditEntry : AutomicTransactionPipeline<WebhookPayloadContext>
    {
        IElasticSearchRepository _elasticSearchRepository;
        public EditEntry(IElasticSearchRepository elasticSearchRepository)
        {
            _elasticSearchRepository = elasticSearchRepository;
        }

        public override async Task<WebhookPayloadContext> ProcessAsync(WebhookPayloadContext input)
        {
            if (input.ServerFeteched == null) throw new MissingFieldException();
            var entry = new Entry(input);

            var serverData = entry.ToDictionary(x => x.Key, x => x.Value);

            await _elasticSearchRepository.UpdateAsync(serverData, input.ElasticSearchId, input.Payload.Model);

            return input;
        }

        public override Task<WebhookPayloadContext> RevertAsync(WebhookPayloadContext input)
        {
            throw new NotImplementedException();
        }
    }
}