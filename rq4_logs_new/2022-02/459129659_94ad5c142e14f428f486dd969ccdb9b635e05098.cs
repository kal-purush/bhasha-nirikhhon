using Moq;
using System.Collections.Generic;
using WebShopLibrary.BusinessAccessLayer.DTOs.MonitorDTO;
using WebShopLibrary.BusinessAccessLayer.Services.ProductServices;
using WebShopLibrary.DataAccessLayer.Database.Entities.Products;
using WebShopLibrary.DataAccessLayer.Database.Entities.Products.Monitors;
using WebShopLibrary.DataAccessLayer.Repositories.ProductRepositories;
using Xunit;

namespace WebShopLibrary.Tests.Services
{
    public class MonitorServiceTests
    {
        private readonly MonitorService _monitorService;
        private readonly Mock<IMonitorRepository> _mockMonitorRepository = new Mock<IMonitorRepository>();

        public MonitorServiceTests()
        {
            _monitorService = new MonitorService(_mockMonitorRepository.Object);
        }

        [Fact]
        public async void GetAllMonitors_ShouldReturnListOfMonitorResponses_WhenMonitorsExist()
        {
            // Arrange
            _mockMonitorRepository
                .Setup(x => x.SelectAllMonitors())
                .ReturnsAsync(MonitorList());

            // Act
            var result = await _monitorService.GetAllMonitors();

            // Assert
            Assert.NotNull(result);
            Assert.IsType<List<MonitorResponse>>(result);
            Assert.Equal(2, result.Count);
        }

        [Fact]
        public async void GetAllMonitors_ShouldReturnEmptyListOfMonitorResponses_WhenNoMonitorsExist()
        {
            // Arrange
            List<Monitor> monitors = new List<Monitor>();

            _mockMonitorRepository
                .Setup(x => x.SelectAllMonitors())
                .ReturnsAsync(monitors);

            // Act
            var result = await _monitorService.GetAllMonitors();

            // Assert
            Assert.NotNull(result);
            Assert.IsType<List<MonitorResponse>>(result);
            Assert.Empty(result);
        }

        [Fact]
        public async void GetMonitorById_ShouldReturnSingleMonitorResponse_WhenMonitorExists()
        {
            // Arrange
            int monitorId = 1;

            _mockMonitorRepository
                .Setup(x => x.SelectMonitorById(It.IsAny<int>()))
                .ReturnsAsync(Monitor());

            // Act
            var result = await _monitorService.GetMonitorById(monitorId);

            // Assert
            Assert.NotNull(result);
            Assert.IsType<MonitorResponse>(result);
            Assert.Equal(monitorId, result.Id);
            Assert.Equal("TestBrand", result.Brand);
            Assert.Equal(1991, result.ReleaseYear);
            Assert.Equal(10, result.Size);
            Assert.Equal(1, result.CategoryId);
            Assert.Equal(1, result.ProductId);
            Assert.IsType<MonitorCategoryResponse>(result.Category);
            Assert.IsType<MonitorProductResponse>(result.Product);
        }

        //[Fact]
        //public async void GetGameById_ShouldReturnNull_WhenGameDoesNotExist()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.SelectGameById(It.IsAny<int>()))
        //        .ReturnsAsync(() => null);

        //    // Act
        //    var result = await _gameService.GetGameById(gameId);

        //    // Assert
        //    Assert.Null(result);
        //}

        //[Fact]
        //public async void UpdateGame_ShouldReturnSingleGameResponse_WhenGameExists()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.UpdateExistingGame(It.IsAny<int>(), It.IsAny<Game>()))
        //        .ReturnsAsync(Game(gameId));

        //    // Act
        //    var result = await _gameService.UpdateGame(gameId, GameRequest());

        //    // Assert
        //    Assert.NotNull(result);
        //    Assert.IsType<GameResponse>(result);
        //    Assert.Equal(gameId, result.Id);
        //    Assert.Equal(1991, result.PublishedYear);
        //}

        //[Fact]
        //public async void UpdateGame_ShouldReturnNull_WhenGameDoesNotExist()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.UpdateExistingGame(It.IsAny<int>(), It.IsAny<Game>()))
        //        .ReturnsAsync(() => null);

        //    // Act
        //    var result = await _gameService.UpdateGame(gameId, GameRequest());

        //    // Assert
        //    Assert.Null(result);
        //}

        //[Fact]
        //public async void CreateGame_ShouldReturnGameResponse_WhenGameIsCreated()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.InsertNewGame(It.IsAny<Game>()))
        //        .ReturnsAsync(Game(gameId));

        //    // Act
        //    var result = await _gameService.CreateGame(GameRequest());

        //    // Assert
        //    Assert.NotNull(result);
        //    Assert.IsType<GameResponse>(result);
        //    Assert.Equal(gameId, result.Id);
        //    Assert.Equal("TestLanguage", result.Language);
        //}

        //[Fact]
        //public async void CreateGame_ShouldReturnNull_WhenGameIsNotCreated()
        //{
        //    // Arrange
        //    _mockGameRepository
        //        .Setup(x => x.InsertNewGame(It.IsAny<Game>()))
        //        .ReturnsAsync(() => null);

        //    // Act
        //    var result = await _gameService.CreateGame(GameRequest());

        //    // Assert
        //    Assert.Null(result);
        //}

        //[Fact]
        //public async void DeleteGame_ShouldReturnGameResponse_WhenGameIsDeleted()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.DeleteGameById(It.IsAny<int>()))
        //        .ReturnsAsync(Game(gameId));

        //    // Act
        //    var result = await _gameService.DeleteGame(gameId);

        //    // Assert
        //    Assert.NotNull(result);
        //    Assert.IsType<GameResponse>(result);
        //    Assert.Equal(gameId, result.Id);
        //    Assert.Equal("TestLanguage", result.Language);
        //}

        //[Fact]
        //public async void DeleteGame_ShouldReturnNull_WhenGameDoesNotExist()
        //{
        //    // Arrange
        //    int gameId = 1;

        //    _mockGameRepository
        //        .Setup(x => x.DeleteGameById(It.IsAny<int>()))
        //        .ReturnsAsync(() => null);

        //    // Act
        //    var result = await _gameService.DeleteGame(gameId);

        //    // Assert
        //    Assert.Null(result);
        //}

        private Monitor Monitor()
        {
            return new Monitor
            {
                Id = 1,
                Brand = "TestBrand",
                ReleaseYear = 1991,
                Size = 10,
                CategoryId = 1,
                ProductId = 1,
                Category = new Category(),
                Product = new Product()
            };
        }

        private List<Monitor> MonitorList()
        {
            return new List<Monitor>()
            {
                new Monitor
                {
                    Id = 1,
                    Brand = "TestBrand",
                    ReleaseYear = 1991,
                    Size = 10,
                    CategoryId = 1,
                    ProductId = 1,
                    Category = new Category(),
                    Product = new Product()
                },
                new Monitor
                {
                    Id = 2,
                    Brand = "TestBrand2",
                    ReleaseYear = 1992,
                    Size = 12,
                    CategoryId = 2,
                    ProductId = 2,
                    Category = new Category(),
                    Product = new Product(),
                }
            };
        }

        private MonitorRequest MonitorRequest()
        {
            return new MonitorRequest
            {
                Brand = "TestBrand",
                ReleaseYear = 1991,
                Size = 10,
                CategoryId = 1,
                ProductId = 1,
            };
        }
    }
}