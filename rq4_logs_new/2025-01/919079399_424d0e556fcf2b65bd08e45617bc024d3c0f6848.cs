using BinanceDashboardAPI.Controllers;
using Interfaces.Services;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Models.DTOs;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Tests.Controllers
{
    public class AccountControllerTests
    {
        private readonly Mock<IBinanceService> _mockBinanceService;
        private readonly AccountController _controller;

        public AccountControllerTests()
        {
            _mockBinanceService = new Mock<IBinanceService>();
            _controller = new AccountController(_mockBinanceService.Object);
        }

        #region GetBalance

        [Fact]
        public async Task Balance_ReturnsExpectedResult()
        {
            // Arrange
            var fakeBalance = new FuturesAccountBalanceResponseDto
            {
                Balance = 1000.00m,
                UpdateTime = DateTime.UtcNow
            };

            _mockBinanceService.Setup(s => s.GetBalanceAsync()).ReturnsAsync(fakeBalance);

            // Act
            var result = await _controller.Balance();

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);  // Expecting OkObjectResult
            Assert.NotNull(okResult);

            var returnedBalance = okResult.Value as FuturesAccountBalanceResponseDto;
            Assert.NotNull(returnedBalance);
            Assert.Equal(fakeBalance.Balance, returnedBalance.Balance);
            Assert.Equal(fakeBalance.UpdateTime, returnedBalance.UpdateTime);
        }

        [Fact]
        public async Task Balance_WhenServiceFails_Returns500StatusCode()
        {
            // Arrange
            _mockBinanceService.Setup(s => s.GetBalanceAsync()).ThrowsAsync(new Exception("Service error"));

            // Act
            var result = await _controller.Balance();

            // Assert
            var objectResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, objectResult.StatusCode);

            var errorResponse = objectResult.Value as dynamic;
            Assert.NotNull(errorResponse);
            Assert.Equal("An error occurred while retrieving the balance.", errorResponse.Message);
        }

        [Fact]
        public async Task Balance_WhenBalanceIsNull_ReturnsNotFound()
        {
            // Arrange
            _mockBinanceService.Setup(s => s.GetBalanceAsync()).ReturnsAsync((FuturesAccountBalanceResponseDto)null);

            // Act
            var result = await _controller.Balance();

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            Assert.Equal(404, notFoundResult.StatusCode);

            var errorResponse = notFoundResult.Value as dynamic;
            Assert.NotNull(errorResponse);
            Assert.Equal("Balance not found.", errorResponse.Message);
        }

        [Fact]
        public async Task Balance_WhenServiceThrowsSpecificException_ReturnsBadRequest()
        {
            // Arrange
            _mockBinanceService.Setup(s => s.GetBalanceAsync()).ThrowsAsync(new InvalidOperationException("Invalid request."));

            // Act
            var result = await _controller.Balance();

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal(400, badRequestResult.StatusCode);

            var errorResponse = badRequestResult.Value as dynamic;
            Assert.NotNull(errorResponse);
            Assert.Equal("Invalid request.", errorResponse.Error);
        }

        [Fact]
        public async Task Balance_WhenServiceTakesTooLong_ReturnsGatewayTimeout()
        {
            // Arrange
            _mockBinanceService.Setup(s => s.GetBalanceAsync()).Returns(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(6)); // Simulate delay
                return new FuturesAccountBalanceResponseDto { Balance = 1000.00m, UpdateTime = DateTime.UtcNow };
            });

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            // Act
            var result = await _controller.Balance();

            // Assert
            var objectResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(504, objectResult.StatusCode);  // Gateway Timeout

            var errorResponse = objectResult.Value as dynamic;
            Assert.NotNull(errorResponse);
            Assert.Equal("The request timed out.", errorResponse.Message);
        }

        [Fact]
        public async Task Balance_ReturnsCorrectResponseType()
        {
            // Arrange
            var fakeBalance = new FuturesAccountBalanceResponseDto
            {
                Balance = 5000.50m,
                UpdateTime = DateTime.UtcNow
            };

            _mockBinanceService.Setup(s => s.GetBalanceAsync()).ReturnsAsync(fakeBalance);

            // Act
            var result = await _controller.Balance();

            // Assert
            Assert.IsType<OkObjectResult>(result);
            var okResult = result as OkObjectResult;
            Assert.NotNull(okResult?.Value);
            Assert.IsType<FuturesAccountBalanceResponseDto>(okResult.Value);
        }


        #endregion GetBalance
    }
}