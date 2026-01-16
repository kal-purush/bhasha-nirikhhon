using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Threading.Tasks;
using TechnicalTest.Server.Controllers;
using TechnicalTest.Server.Services;
using TechnicalTest.Shared;

namespace TechnicalTest.Tests
{
    [ExcludeFromCodeCoverage]
    [TestClass]
    public class ReportControllerTests
    {
        [TestMethod]
        public async Task GenerateAsync_HappyScenario_ReturnsAccepted()
        {
            // Arrange
            var mockReportGenerator = new Mock<IReportGenerator>();
            mockReportGenerator.Setup(m => m.GenerateAsync()).ReturnsAsync(It.IsAny<ReportFinal>());

            var controller = new ReportController(mockReportGenerator.Object);

            var expectedStatusCode = HttpStatusCode.Accepted;

            // Act
            var actual = await controller.GenerateAsync();
            var code = (HttpStatusCode)actual.GetType().GetProperty("StatusCode").GetValue(actual, null);

            // Assert
            code.Should().BeEquivalentTo(expectedStatusCode);
        }

        [TestMethod]
        public void ReportController_WithNullGenerator_ThrowsArgumentNullException()
        {
            // Arrange
            IReportGenerator reportGenerator = null;

            // Act
            Action initFunction = () => new ReportController(reportGenerator);

            // Assert
            initFunction.Should().ThrowExactly<ArgumentNullException>(nameof(reportGenerator));
        }
    }
}