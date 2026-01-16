using Autofac;
using Moq;
using System.Collections.Specialized;
using WordCount.Abstractions.SystemAbstractions.Configuration;
using WordCount.Implementations;
using WordCount.Tests.XUnitHelpers;
using Xunit;

namespace WordCount.Tests.SystemAbstractionTests.ConfigurationTests
{
    public class AppSettingsReaderTests
    {
        private readonly Mock<IConfigurationManager> _mockConfigurationManager;
        private readonly AppSettingsReader _systemUnderTest;

        public AppSettingsReaderTests()
        {
            _mockConfigurationManager = new Mock<IConfigurationManager>();

            ContainerBuilder containerBuilder = new ContainerBuilder();

            containerBuilder
                .RegisterInstance(instance: _mockConfigurationManager.Object)
                .As<IConfigurationManager>();

            containerBuilder
                .RegisterType<AppSettingsReader>();

            _systemUnderTest = containerBuilder
                .Build()
                .Resolve<AppSettingsReader>();
        }

        [NamedFact]
        public void AppSettingsReaderTests_DefaultLanguage_Expect_key_call_defaultLanguage()
        {
            NameValueCollection nameValueCollection = new NameValueCollection
            {
                { "defaultLanguage", "de" }
            };
            _mockConfigurationManager
                .SetupGet(expression: m => m.AppSettings)
                .Returns(value: nameValueCollection);

            string actual = _systemUnderTest.DefaultLanguage;

            Assert.Equal(expected: "de", actual: actual);

            _mockConfigurationManager
                .Verify(expression: v => v.AppSettings, times: Times.Once);
        }
    }
}