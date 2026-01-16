using System;
using Microsoft.AspNetCore.Mvc;
using Moq;
using TondForooshApi.Controllers;
using TondForooshApi.Models;

namespace TondForooshApi.Tests;

public class HomeControllerTests
{
    [Fact]
    public void Can_Use_Repository()
    {
        // Arrange
        Mock<ITondForooshRepository> mock = new();
        mock.Setup(m => m.Products).Returns((new Product[] {
            new Product {Id=1, Name="P1"},
            new Product {Id=2, Name="P2"}
        }).AsQueryable<Product>());

        HomeController controller = new(mock.Object);

        // Act
        var result = controller.GetProducts();
        var okResult = result.Result as OkObjectResult;
        var products = okResult?.Value as IEnumerable<Product>;

        // Assert
        Assert.NotNull(okResult);
        Assert.NotNull(products);
        Product[] productArray = products.ToArray();
        Assert.True(productArray.Length == 2);
        Assert.Equal("P1", productArray[0].Name);
        Assert.Equal("P2", productArray[1].Name);
    }
}