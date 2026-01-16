using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;

namespace HostedQueueProcessor.Core
{
    public class ConfigValuesResolver : INameResolver
    {
        private readonly IConfiguration _configuration;

        public ConfigValuesResolver(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public string Resolve(string name)
        {
            var value = _configuration.GetValue<string>(name);
            return value;

        }
    }
}