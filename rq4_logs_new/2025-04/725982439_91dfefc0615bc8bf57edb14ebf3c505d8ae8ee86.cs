using FluentAssertions;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FrontendAccountCreation.Web.UnitTests.FeatureFlag;

[TestClass]
public class FeatureFlagTests
{
    private IConfiguration configuration;

    [TestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public void ShowYourFeedbackFooter_Should_Be_True(bool value)
    {
        // Arrange
        var inMemorySettings = new Dictionary<string, string>
        {
            {FrontendAccountCreation.Web.Constants.FeatureFlags.ShowYourFeedbackFooter, value.ToString()}
        };

        configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        // Act
        bool showYourFeedbackFooter = bool.Parse(configuration[FrontendAccountCreation.Web.Constants.FeatureFlags.ShowYourFeedbackFooter]);

        // Assert
        showYourFeedbackFooter.Should().Be(value);
    }
}