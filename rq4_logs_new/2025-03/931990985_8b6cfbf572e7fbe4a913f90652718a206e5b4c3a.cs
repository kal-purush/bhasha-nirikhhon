using Elastic.Ingest.Elasticsearch;
using Elastic.Ingest.Elasticsearch.DataStreams;
using Elastic.Serilog.Sinks;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Exceptions;

namespace UserService.DataAccess.DI
{
    public static class LoggingConfiguration
    {
        public static void ConfigureLogging(IConfiguration configuration)
        {
            var elasticSearchUri = configuration["ElasticSearchUri"];
            var logstashUri = configuration["LogstashUri"];

            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails() 
                .WriteTo.Console()
                .WriteTo.Elasticsearch([new Uri(elasticSearchUri!)], options =>
                {
                    options.DataStream = new DataStreamName("UserService-DataStream");
                    options.BootstrapMethod = BootstrapMethod.Failure;
                })
                .WriteTo.Http(logstashUri!, queueLimitBytes: null)
                .CreateLogger();
        }
    }
}