using BetterCoding.MessagePubSubCenter.Services;
using BetterCoding.Strapi.SDK.Core.Webhook;
using MassTransit;
using Newtonsoft.Json;

namespace BetterCoding.MessagePubSubCenter.Workers.Consumers
{
    public class StrapiWebhookConsumer : IConsumer<WebhookPayload>
    {
        private readonly ILogger<StrapiWebhookConsumer> _logger;
        private readonly IStrapiWebhookService _strapiWebhookService;
        public StrapiWebhookConsumer(ILogger<StrapiWebhookConsumer> logger,
            IStrapiWebhookService strapiWebhookService)
        {
            _logger = logger;
            _strapiWebhookService = strapiWebhookService;
        }

        public async Task Consume(ConsumeContext<WebhookPayload> context)
        {
            var json = JsonConvert.SerializeObject(context.Message);
            _logger.LogInformation("Received Json: {json}", json);
            await _strapiWebhookService.SyncToElasticSearch(context.Message);
        }
    }

    public class StrapiWebhookConsumerDefinition : ConsumerDefinition<StrapiWebhookConsumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
            IConsumerConfigurator<StrapiWebhookConsumer> consumerConfigurator)
        {
            endpointConfigurator.UseMessageRetry(r => r.Intervals(3000, 5000, 10000));
        }
    }
}