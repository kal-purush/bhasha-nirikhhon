using System.Collections.Generic;
using System.Linq;
using MAF.FeaturesFlipping.Activators.Configuration;
using MAF.FeaturesFlipping.Extensibility.Activators;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace MAF.FeaturesFlipping.Extensions.DependencyInjection.Configuration.UnitTests
{
    public partial class FeaturesFlippingBuilderGlobalConfigurationExtensionsTests
    {
        [Trait("Category", "UnitTest")]
        public class AddGlobalConfigurationActivator
        {
            [Fact]
            public void A()
            {
                // Arrange
                var serviceCollection = new ServiceCollection();
                var featuresFlippingBuilderMock = new Mock<IFeaturesFlippingBuilder>();
                featuresFlippingBuilderMock.SetupGet(_ => _.Services)
                    .Returns(serviceCollection);
                var configurationSection = new ConfigurationSection(
                    new ConfigurationRoot(
                        new List<IConfigurationProvider>()),
                    "somePath");
                var expectedLifetime = ServiceLifetime.Singleton;

                // Act
                var actualFeaturesFlippingBuilder =
                    featuresFlippingBuilderMock.Object.AddGlobalConfigurationActivator(configurationSection);

                // Assert
                Assert.NotNull(actualFeaturesFlippingBuilder);
                Assert.Equal(serviceCollection, actualFeaturesFlippingBuilder.Services);
                Assert.Equal(1, actualFeaturesFlippingBuilder.Services.Count);
                var actualServiceDescriptor = actualFeaturesFlippingBuilder.Services.First();
                Assert.Equal(typeof(IFeatureActivator), actualServiceDescriptor.ServiceType);
                Assert.IsType<GlobalConfigurationFeatureActivator>(actualServiceDescriptor.ImplementationInstance);
                Assert.Equal(expectedLifetime, actualServiceDescriptor.Lifetime);
                Assert.Equal(configurationSection,
                    ((GlobalConfigurationFeatureActivator) actualServiceDescriptor.ImplementationInstance)
                    .ConfigurationSection);
            }
        }
    }
}