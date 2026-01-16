using System;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using CentralLogProvider;
using Microsoft.Extensions.Logging;

namespace CentralLogger.Tests {
    public class ProviderTests {
        [Fact]
        public void ShouldRegisterLogProvider() {
            var collection = new ServiceCollection();
            collection.AddLogging(builder => {
                builder.ClearProviders();
                builder.AddCentralLog(new CentralLogOptions("http://localhost:5000"));
            });
            var provider = collection.BuildServiceProvider();
            var logger = provider.GetService<ILogger<ProviderTests>>();
            Assert.NotNull(logger);
        }
    }
}