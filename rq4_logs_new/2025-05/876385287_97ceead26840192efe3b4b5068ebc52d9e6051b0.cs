using LangSharp.Core.Configuration;
using LangSharp.Core.Enums;
using LangSharp.Core.Interfaces.Services;
using LangSharp.MicrosoftExtensionsDI;
using LangSharp.Registrations;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace LangSharp.UnitTests.MicrosoftExtensionsDI
{
    public class ServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddLangSharp_ShouldThrowArgumentException_WhenConfigurationIsNull()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => services.AddLangSharp(null!));
            Assert.Equal("No configuration found.", exception.Message);
        }

        [Fact]
        public void AddLangSharp_ShouldAddConfigurationAsSingleton()
        {
            // Arrange
            var services = new ServiceCollection();
            var configuration = new LangSharpConfiguration
            {
                AIProvider = AIProviderType.OpenAI,
                ApiKey = "test-api-key",
                Model = "test-model",
                DatabaseUri = "test-database-uri"
            };

            // Act
            services.AddLangSharp(configuration);

            // Assert
            var serviceDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(LangSharpConfiguration));
            Assert.NotNull(serviceDescriptor);
            Assert.Equal(ServiceLifetime.Singleton, serviceDescriptor.Lifetime);
            Assert.Same(configuration, serviceDescriptor.ImplementationInstance);
        }

        [Fact]
        public void AddLangSharp_ShouldReturnServiceCollection()
        {
            // Arrange
            var services = new ServiceCollection();
            var configuration = new LangSharpConfiguration
            {
                AIProvider = AIProviderType.OpenAI,
                ApiKey = "test-api-key",
                Model = "test-model",
                DatabaseUri = "test-database-uri"
            };

            // Act
            var result = services.AddLangSharp(configuration);

            // Assert
            Assert.Same(services, result);
        }
    }
}