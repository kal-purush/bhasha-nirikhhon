using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dfe.Spi.Common.Logging.Definitions;
using Dfe.Spi.Common.WellKnownIdentifiers;
using Dfe.Spi.UkrlpAdapter.Domain.Configuration;
using Dfe.Spi.UkrlpAdapter.Domain.Translation;
using Newtonsoft.Json.Linq;
using RestSharp;

namespace Dfe.Spi.UkrlpAdapter.Infrastructure.SpiTranslator
{
    public class TranslatorApiClient : ITranslator
    {
        private readonly IRestClient _restClient;
        private readonly ILoggerWrapper _logger;

        public TranslatorApiClient(
            IRestClient restClient,
            TranslatorConfiguration configuration,
            ILoggerWrapper logger)
        {
            _restClient = restClient;
            _restClient.BaseUrl = new Uri(configuration.BaseUrl);
            if (!string.IsNullOrEmpty(configuration.FunctionsKey))
            {
                _restClient.DefaultParameters.Add(
                    new Parameter("x-functions-key", configuration.FunctionsKey, ParameterType.HttpHeader));
            }
            
            _logger = logger;
        }

        public async Task<string> TranslateEnumValue(string enumName, string sourceValue,
            CancellationToken cancellationToken)
        {
            var resource = $"enumerations/{enumName}/{SourceSystemNames.UkRegisterOfLearningProviders}";
            _logger.Info($"Calling {resource} on translator api");
            var request = new RestRequest(resource, Method.GET);
            var response = await _restClient.ExecuteTaskAsync(request, cancellationToken);
            if (!response.IsSuccessful)
            {
                throw new TranslatorApiException(resource, response.StatusCode, response.Content);
            }

            _logger.Info($"Received {response.Content}");
            var root = JObject.Parse(response.Content);
            var mappingsResult = (JObject) root["mappingsResult"];
            var mappings = (JObject) mappingsResult["mappings"];
            var mapping = mappings.Properties()
                .FirstOrDefault(p =>
                    ((JArray) p.Value).Any(i =>
                        ((string) i).Equals(sourceValue, StringComparison.InvariantCultureIgnoreCase)));
            if (mapping == null)
            {
                _logger.Info($"No enum mapping found for {SourceSystemNames.UkRegisterOfLearningProviders} for {enumName} with value {sourceValue}");
                return null;
            }
            
            _logger.Debug($"Found mapping of {mapping.Name} for {enumName} with value {sourceValue}");
            return mapping.Name;
        }
    }
}