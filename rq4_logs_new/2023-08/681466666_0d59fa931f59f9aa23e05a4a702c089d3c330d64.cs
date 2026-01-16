using BetterCoding.Strapi.SDK.Core;
using EasyNetQ;
using Newtonsoft.Json;

namespace BetterCoding.MessagePubSubCenter.Workers.Consumers
{
    public class StrapiElasticSearchSync : BackgroundService
    {
        private readonly ILogger<StrapiElasticSearchSync> _logger;
        private readonly IBus _bus;

        public StrapiElasticSearchSync(ILogger<StrapiElasticSearchSync> logger,
            IBus bus)
        {
            _logger = logger;
            _bus = bus;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _bus.PubSub.SubscribeAsync<StrapiWebhookPayload>("sync-strapi-to-elasticsearch", Handle);
        }

        private async Task Handle(StrapiWebhookPayload message)
        {
            var json = JsonConvert.SerializeObject(message);
            _logger.LogInformation($"sync strapi data to elasticsearch: {json}");
        }
    }
}