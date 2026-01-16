using BusinessCardInformation.Core.IServices;
using BusinessCardInformation.Core.Models.Request;
using Microsoft.AspNetCore.Mvc;
using Moq;
using ResturantWebSite.API.Controllers;
using System.Text;

namespace UnitTest
{
    public class BusinessCardControllerTests
    {
        private readonly Mock<IBusinessCardServices> _mockService;
        private readonly BusinessCardController _controller;

        public BusinessCardControllerTests()
        {
            _mockService = new Mock<IBusinessCardServices>();
            _controller = new BusinessCardController(_mockService.Object);
        }

        [Fact]
        public async Task Create_ReturnsOk_WhenSuccessful()
        {
            // Arrange
            var dto = new BusinessCardDTO
            {
                Name = "Test",
                Gender = "Male",
                DateOfBirth = DateTime.Now.AddYears(-25),
                Email = "test@example.com",
                Phone = "1234567890",
                Photo = "photo.jpg",
                Address = "123 Street"
            };

            _mockService.Setup(s => s.Create(dto)).ReturnsAsync(dto);

            // Act
            var result = await _controller.Create(dto);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            Assert.Equal(dto, okResult.Value);
        }

        [Fact]
        public async Task Create_ReturnsBadRequest_WhenResultIsNull()
        {
            // Arrange
            var dto = new BusinessCardDTO
            {
                Name = "dd",
                Gender = "Male",
                DateOfBirth = DateTime.Now.AddYears(-25),
                Email = "test@example.com",
                Phone = "1234567890",
                Photo = "photo.jpg",
                Address = "123 Street"
            };
            _mockService.Setup(s => s.Create(dto)).ReturnsAsync((BusinessCardDTO?)null);

            // Act
            var result = await _controller.Create(dto);

            // Assert
            Assert.IsType<BadRequestObjectResult>(result);
        }

        [Fact]
        public async Task Delete_ReturnsBadRequest_WhenDeletionFails()
        {
            // Arrange
            int id = 99;
            _mockService.Setup(s => s.Delete(id)).ReturnsAsync(false);

            // Act
            var result = await _controller.Delete(id);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal(false, badRequestResult.Value);
        }

        [Fact]
        public async Task Delete_ReturnsOk_WhenDeletionSuccessful()
        {
            // Arrange
            int id = 1;
            _mockService.Setup(s => s.Delete(id)).ReturnsAsync(true);

            // Act
            var result = await _controller.Delete(id);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            Assert.Equal(true, okResult.Value);
        }


        [Fact]
        public async Task CreateBulk_ReturnsBadRequest_WhenServiceReturnsNull()
        {
            // Arrange
            var list = new List<BusinessCardDTO>
            {
                new BusinessCardDTO { Name = "Test", Email = "test@test.com", Gender = "male", DateOfBirth = DateTime.Now.AddYears(-20), Phone = "1111111111", Address = "Test" }
            };

            _mockService.Setup(s => s.CreateBulk(list)).ReturnsAsync((List<BusinessCardDTO>?)null);

            // Act
            var result = await _controller.CreateBulk(list);

            // Assert
            var badRequest = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Null(badRequest.Value);
        }

        [Fact]
        public async Task CreateBulk_ReturnsOk_WhenSuccessful()
        {
            // Arrange
            var list = new List<BusinessCardDTO>
            {
                new BusinessCardDTO { Name = "Test", Email = "Test@Test.com", Gender = "Male", DateOfBirth = DateTime.Now.AddYears(-30), Phone = "1234567890", Address = "Test" },
                new BusinessCardDTO { Name = "Test", Email = "Test@Test.com", Gender = "Female", DateOfBirth = DateTime.Now.AddYears(-25), Phone = "0987654321", Address = "Test" }
            };

            _mockService.Setup(s => s.CreateBulk(list)).ReturnsAsync(list);

            // Act
            var result = await _controller.CreateBulk(list);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            Assert.Equal(list, okResult.Value);
        }


        [Fact]
        public async Task ExportToCSV_ReturnsBadRequest_WhenFilterIsInvalid()
        {
            // Arrange
            var result = await _controller.ExportToCSV(null);

            // Assert
            var badRequestResult = Assert.IsType<BadRequestObjectResult>(result);
            Assert.Equal("Invalid filter parameters.", badRequestResult.Value);
        }


        [Fact]
        public async Task ExportToCSV_ReturnsNoContent_WhenNoDataFound()
        {
            // Arrange
            var emptyResult = new PageResult<BusinessCardDTO>
            {
                Collection = new List<BusinessCardDTO>() 
            };

            _mockService.Setup(s => s.GetAll(It.IsAny<BusinessCardFilter>())).ReturnsAsync(emptyResult);

            // Act
            var result = await _controller.ExportToCSV(new BusinessCardFilter());

            // Assert
            Assert.IsType<NoContentResult>(result);
        }


        [Fact]
        public async Task ExportToCSV_ReturnsInternalServerError_WhenServiceFails()
        {
            // Arrange
            _mockService.Setup(s => s.GetAll(It.IsAny<BusinessCardFilter>())).ThrowsAsync(new Exception("Database failure"));

            // Act
            var result = await _controller.ExportToCSV(new BusinessCardFilter());

            // Assert
            var statusCodeResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, statusCodeResult.StatusCode);
            Assert.Equal("An error occurred while processing your request.", statusCodeResult.Value);
        }


    }
}