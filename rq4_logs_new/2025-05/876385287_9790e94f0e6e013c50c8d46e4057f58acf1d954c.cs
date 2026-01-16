using LangSharp.Core.Configuration;
using LangSharp.Core.Enums;
using LangSharp.Core.Interfaces.Services;
using LangSharp.MicrosoftExtensionsDI;
using Microsoft.Extensions.DependencyInjection;

namespace LangSharp.IntegrationTests.Fixtures
{
    public abstract class LangSharpServiceFixtureBase
    {
        public ILangSharpService Service { get; }

        protected LangSharpServiceFixtureBase(string? apiKey = null, string? model = null, AIProviderType? provider = null)
        {
            var services = new ServiceCollection();

            apiKey ??=Environment.GetEnvironmentVariable("LANGSHARP_TEST_API_KEY");
            model ??= "gpt-4o-mini";
            provider ??= AIProviderType.LangChain;

            if (string.IsNullOrWhiteSpace(apiKey))
                throw new InvalidOperationException("LANGSHARP_API_KEY environment variable is not set.");

            var config = new LangSharpConfigurationBuilder()
                .SetAIProvider(provider.Value)
                .SetModel(model)
                .SetApiKey(apiKey)
                .Build();

            services.AddLangSharp(config);

            var providerInstance = services.BuildServiceProvider();
            Service = providerInstance.GetRequiredService<ILangSharpService>();
        }
    }
}