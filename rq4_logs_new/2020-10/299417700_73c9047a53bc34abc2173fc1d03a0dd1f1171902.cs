using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SNSEvents;
using Nest;
using Newtonsoft.Json;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
namespace WebAdvertisements.SearchWorker
{
    public class SearchWorker
    {
        public SearchWorker() : this(ElasticSearchHelper.GetInstance())
        {

        }

        private readonly IElasticClient _client;
        public SearchWorker(IElasticClient client)
        {
            _client = client;
        }

        public async Task Function(SNSEvent snsEvent, ILambdaContext context)
        {
            foreach (var record in snsEvent.Records)
            {
                context.Logger.LogLine(record.Sns.Message);

                var message = JsonConvert.DeserializeObject<AdvertConfirmedMessage>(record.Sns.Message);
                var advertDocument = MappingHelper.Map(message);
                await _client.IndexDocumentAsync(advertDocument);
                context.Logger.LogLine(JsonConvert.SerializeObject(message));
            }
        }
    }
}