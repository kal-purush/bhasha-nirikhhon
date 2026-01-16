using EasyTravel.Domain.Services;
using EasyTravel.Domain.Entites;
using Microsoft.AspNetCore.Mvc;
using Moq;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyTravel.Web.Controllers;

namespace UnitTest
{
    [TestFixture]
    public class RecommendationControllerTests
    {
        private Mock<IRecommendationService> _recommendationServiceMock;
        private RecommendationController _controller;

        [SetUp]
        public void SetUp()
        {
            _recommendationServiceMock = new Mock<IRecommendationService>();
            _controller = new RecommendationController(_recommendationServiceMock.Object);
        }
        [TearDown]
        public void TearDown()
        {
            if (_controller is IDisposable disposableController)
            {
                disposableController.Dispose();
            }
        }


        [Test]
        public async Task Get_ReturnsOkResult_WithRecommendations()
        {
            // Arrange
            var type = "hotel";
            var count = 5;
            var recommendations = new List<RecommendationDto>
    {
        new RecommendationDto { Title = "Hotel A", ImageUrl = "image1.jpg", Description = "Description A", Rating = 4.5, Location = "Location A" },
        new RecommendationDto { Title = "Hotel B", ImageUrl = "image2.jpg", Description = "Description B", Rating = 4.0, Location = "Location B" }
    };

            _recommendationServiceMock
                .Setup(service => service.GetRecommendationsAsync(type, count))
                .ReturnsAsync(recommendations);

            // Act
            var result = await _controller.Get(type, count);

            // Assert
            Assert.That(result, Is.TypeOf<OkObjectResult>());
            var okResult = result as OkObjectResult;
            Assert.That(okResult, Is.Not.Null);
            Assert.That(okResult.StatusCode, Is.EqualTo(200));
            Assert.That(okResult.Value, Is.EqualTo(recommendations));
        }

        [Test]
        public async Task Get_ReturnsOkResult_WithEmptyList_WhenNoRecommendations()
        {
            // Arrange
            var type = "car";
            var count = 3;
            var recommendations = new List<RecommendationDto>();

            _recommendationServiceMock
                .Setup(service => service.GetRecommendationsAsync(type, count))
                .ReturnsAsync(recommendations);

            // Act
            var result = await _controller.Get(type, count);

            // Assert
            Assert.That(result, Is.TypeOf<OkObjectResult>());
            var okResult = result as OkObjectResult;
            Assert.That(okResult, Is.Not.Null);
            Assert.That(okResult.StatusCode, Is.EqualTo(200));
            Assert.That(okResult.Value, Is.EqualTo(recommendations));
        }

        [Test]
        public async Task Get_ReturnsBadRequest_WhenExceptionIsThrown()
        {
            // Arrange
            var type = "invalid";
            var count = 5;

            _recommendationServiceMock
                .Setup(service => service.GetRecommendationsAsync(type, count))
                .ThrowsAsync(new System.Exception("Error occurred"));

            // Act
            var result = await _controller.Get(type, count);

            // Assert
            Assert.That(result, Is.TypeOf<ObjectResult>());
            var objectResult = result as ObjectResult;
            Assert.That(objectResult, Is.Not.Null);
            Assert.That(objectResult.StatusCode, Is.EqualTo(500));
            Assert.That(objectResult.Value, Is.EqualTo("Error occurred"));
        }

    }
}