using Goal.Application.Mappings;
using Goal.Application.Services.Product;
using Goal.Infrastructure.Repositories.Interfaces.Product;
using Goal.Shared.DTOs.Product;
using Goal.Shared.Models.Response.Product;
using FluentAssertions;
using Goal.Shared.Models.Base;
using Moq;

namespace Goal.Tests.UnitTests.Goal;

public class BusinessGoalServiceTests
{
    private readonly Mock<IProductRepository> _mockProductRepository;
    private readonly ProductService _service;

    public BusinessGoalServiceTests()
    {
        _mockProductRepository = new Mock<IProductRepository>();
        _service = new ProductService(_mockProductRepository.Object, new ApplicationMapper());
    }

    [Fact]
    public async Task GetAllProductsAsync_ShouldReturnAllProducts_WhenProductsExist()
    {
        // Arrange
        _mockProductRepository
            .Setup(x => x.GetAllAsync())
            .ReturnsAsync(StaticProductList);

        // Act
        var result = (await _service.GetAllProductsAsync() ?? Array.Empty<ProductResponse>()).ToList();

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(StaticProductList.Count);
        _mockProductRepository.Verify(x => x.GetAllAsync(), Times.Once);
    }

    [Fact]
    public async Task GetAllProductsAsync_ShouldThrowException_WhenRepositoryFails()
    {
        // Arrange
        _mockProductRepository
            .Setup(x => x.GetAllAsync())
            .ThrowsAsync(new Exception());

        // Act
        Func<Task> act = async () => await _service.GetAllProductsAsync();

        // Assert
        await act.Should().ThrowAsync<Exception>();
        _mockProductRepository.Verify(x => x.GetAllAsync(), Times.Once);
    }

    [Fact]
    public async Task GetProductByIdAsync_ShouldCallRepositoryMethod_WithCorrectId()
    {
        // Arrange
        const int productId = 42;
        _mockProductRepository.Setup(x => x.GetByIdAsync(productId))
            .ReturnsAsync(new ProductDto { Id = productId });

        // Act
        var result = await _service.GetProductByIdAsync(productId);

        // Assert
        result.Should().NotBeNull();
        result.Id.Should().Be(productId);
        _mockProductRepository.Verify(x => x.GetByIdAsync(productId), Times.Once);
    }

    [Fact]
    public async Task GetAllProductsPagedAsync_ShouldReturnCorrectPage_WhenCalledWithValidArguments()
    {
        // Arrange
        const int pageNumber = 2;
        const int pageSize = 5;

        var expectedProducts = StaticProductList
            .Skip((pageNumber - 1) * pageSize)
            .Take(pageSize)
            .ToList();

        var pagedResult = new PagedResult<ProductDto>
        {
            Items = expectedProducts,
            PageNumber = pageNumber,
            PageSize = pageSize,
            TotalItems = StaticProductList.Count
        };

        _mockProductRepository
            .Setup(x => x.GetPagedAsync(pageNumber, pageSize))
            .ReturnsAsync(pagedResult);

        // Act
        var result = await _service.GetAllProductsPagedAsync(pageNumber, pageSize);

        // Assert
        result.Should().NotBeNull();
        result.PageNumber.Should().Be(pageNumber);
        result.PageSize.Should().Be(pageSize);
        result.TotalItems.Should().Be(StaticProductList.Count);
        result.Items.Should().HaveCount(pageSize);
        result.Items.First().Id.Should().Be(expectedProducts.First().Id);
        result.Items.Last().Id.Should().Be(expectedProducts.Last().Id);
        _mockProductRepository.Verify(x => x.GetPagedAsync(pageNumber, pageSize), Times.Once);
    }

    private static readonly List<ProductDto> StaticProductList =
    [
        new() { Id = 1, Name = "Produkt 1", Description = "Popis produktu 1", Price = 10.99m },
        new() { Id = 2, Name = "Produkt 2", Description = "Popis produktu 2", Price = 12.49m },
        new() { Id = 3, Name = "Produkt 3", Description = "Popis produktu 3", Price = 8.79m },
        new() { Id = 4, Name = "Produkt 4", Description = "Popis produktu 4", Price = 15.00m },
        new() { Id = 5, Name = "Produkt 5", Description = "Popis produktu 5", Price = 5.49m },
        new() { Id = 6, Name = "Produkt 6", Description = null, Price = 20.00m },
        new() { Id = 7, Name = "Produkt 7", Description = "Popis produktu 7", Price = 18.99m },
        new() { Id = 8, Name = "Produkt 8", Description = "Popis produktu 8", Price = 11.49m },
        new() { Id = 9, Name = "Produkt 9", Description = "Popis produktu 9", Price = 9.99m },
        new() { Id = 10, Name = "Produkt 10", Description = null, Price = 3.49m },
        new() { Id = 11, Name = "Produkt 11", Description = "Popis produktu 11", Price = 7.99m },
        new() { Id = 12, Name = "Produkt 12", Description = "Popis produktu 12", Price = 25.00m }
    ];
}