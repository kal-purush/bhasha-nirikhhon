using BetterCoding.MessagePubSubCenter.Repository.ElasticSearch;
using BetterCoding.MessagePubSubCenter.Services.Pipelines.Webhook;
using BetterCoding.Patterns.Pipeline;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;

namespace BetterCoding.MessagePubSubCenter.Services.Pipelines
{
    public class CreateIndex : AutomicTransactionPipeline<WebhookPayloadContext>
    {
        IElasticSearchRepository _elasticSearchRepository;
        public CreateIndex(IElasticSearchRepository elasticSearchRepository)
        {
            _elasticSearchRepository = elasticSearchRepository;
        }

        public override async Task<WebhookPayloadContext> ProcessAsync(WebhookPayloadContext input)
        {
            //if (input.IndexExist) return input;

            var entry = new Entry(input);
            var specificIdField = entry.TryGetValue("id", out var id);
            if (!specificIdField) return input;

            var settings = new IndexSettings();
            
            var createIndexRequest = new CreateIndexRequest("test-create-index")
            {
                Settings = new IndexSettings(),
            };
            
            var response = await _elasticSearchRepository.Client.Indices.CreateAsync(createIndexRequest);

            return input;
        }

        class IdProperty : IProperty
        {
            public string Type { get; } = "_id";
        }

        public override Task<WebhookPayloadContext> RevertAsync(WebhookPayloadContext input)
        {
            throw new NotImplementedException();
        }
    }
}