using Autofac;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Configuration;

namespace BetterCoding.MessagePubSubCenter.Repository
{
    public static class AutofacExtensions
    {
        public static ContainerBuilder UseElasticSearch(this ContainerBuilder builder, IConfiguration configuration)
        {
            var elasticsearchUrl = configuration.GetConnectionString("ElasticSearch");
            if (elasticsearchUrl == null)
            {
                throw new Exception("no specific elasticsearch url found.");
            }

            var settings = new ElasticsearchClientSettings(new Uri(elasticsearchUrl))
                .EnableDebugMode()
                .PrettyJson()
                .RequestTimeout(TimeSpan.FromMinutes(2));

            var client = new ElasticsearchClient(settings);
            
            builder.RegisterInstance(client).As<ElasticsearchClient>().SingleInstance();

            return builder;
        }
    }
}