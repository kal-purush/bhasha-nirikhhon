using Microsoft.AspNetCore.Mvc;
using Moq;
using WebApi.Controllers;
using Core.Entities;
using MockQueryable;
using Core.DTOs;
using Infrastructure.Data;

namespace TondForooshApi.Tests;

public class CategoryControllerTests
{
    #region GetCategories
    [Fact]
    public async Task GetCategories_ShouldReturnAllCategories_WhenCategoriesExist()
    {
        // Arrange
        var categories = new List<Category>
        {
            new Category { Id = 1, Name = "Category1" },
            new Category { Id = 2, Name = "Category2" }
        };

        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(categories.AsQueryable().BuildMock());

        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.GetCategories();
        var okResult = result.Result as OkObjectResult;
        var returnedCategories = okResult?.Value as IEnumerable<Category>;

        // Assert
        Assert.NotNull(okResult);
        Assert.NotNull(returnedCategories);
        Assert.Equal(2, returnedCategories.Count());
        Assert.Equal("Category1", returnedCategories.First().Name);
    }
    #endregion

    #region GetCategory
    [Fact]
    public async Task GetCategory_ShouldReturnCategory_WhenIdExists()
    {
        // Arrange
        var category = new Category { Id = 1, Name = "Category1" };
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category> { category }.AsQueryable().BuildMock());

        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.GetCategory(1);
        var okResult = result.Result as OkObjectResult;
        var returnedCategory = okResult?.Value as Category;

        // Assert
        Assert.NotNull(okResult);
        Assert.NotNull(returnedCategory);
        Assert.Equal(category.Name, returnedCategory.Name);
    }

    [Fact]
    public async Task GetCategory_ShouldReturnNotFound_WhenIdDoesNotExist()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category>().AsQueryable().BuildMock());

        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.GetCategory(1);

        // Assert
        Assert.IsType<NotFoundResult>(result.Result);
    }
    #endregion

    #region CreateCategory
    [Fact]
    public async Task CreateCategory_ShouldCreateNewCategory_WhenValidDataProvided()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.AddCategoryAsync(It.IsAny<Category>()))
            .ReturnsAsync(1);

        var controller = new CategoryController(mock.Object);
        var createDto = new CreateCategoryDto("Test Category");

        // Act
        var result = await controller.CreateCategory(createDto);
        var okResult = result.Result as OkObjectResult;
        var categoryId = (int)okResult!.Value;

        // Assert
        Assert.NotNull(okResult);
        Assert.Equal(1, categoryId);
    }

    [Fact]
    public async Task CreateCategory_ShouldReturnBadRequest_WhenDtoIsNull()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.CreateCategory(null!);

        // Assert
        Assert.IsType<BadRequestResult>(result.Result);
    }
    #endregion

    #region DeleteCategory
    [Fact]
    public async Task DeleteCategory_ShouldReturnNoContent_WhenCategoryExists()
    {
        // Arrange
        var category = new Category { Id = 1, Name = "Category1" };
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category> { category }.AsQueryable().BuildMock());
        mock.Setup(m => m.DeleteCategoryAsync(It.IsAny<Category>())).Returns(Task.CompletedTask);

        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.DeleteCategory(1);

        // Assert
        Assert.IsType<NoContentResult>(result);
    }

    [Fact]
    public async Task DeleteCategory_ShouldReturnNotFound_WhenCategoryDoesNotExist()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category>().AsQueryable().BuildMock());

        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.DeleteCategory(1);

        // Assert
        Assert.IsType<NotFoundResult>(result);
    }
    #endregion

    #region UpdateCategory
    [Fact]
    public async Task UpdateCategory_ShouldReturnNoContent_WhenCategoryIsUpdatedSuccessfully()
    {
        // Arrange
        var category = new Category { Id = 1, Name = "Old Name" };
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category> { category }.AsQueryable().BuildMock());
        mock.Setup(m => m.UpdateCategoryAsync(It.IsAny<Category>())).Returns(Task.CompletedTask);
        var controller = new CategoryController(mock.Object);
        var updateDto = new UpdateCategoryDto { Id = 1, Name = "New Name" };

        // Act
        var result = await controller.UpdateCategory(updateDto);

        // Assert
        Assert.IsType<NoContentResult>(result);
        Assert.Equal("New Name", category.Name);
    }

    [Fact]
    public async Task UpdateCategory_ShouldReturnNotFound_WhenCategoryDoesNotExist()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category>().AsQueryable().BuildMock());
        var controller = new CategoryController(mock.Object);
        var updateDto = new UpdateCategoryDto { Id = 1, Name = "New Name" };

        // Act
        var result = await controller.UpdateCategory(updateDto);

        // Assert
        Assert.IsType<NotFoundResult>(result);
    }

    [Fact]
    public async Task UpdateCategory_ShouldReturnBadRequest_WhenDtoIsNull()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        var controller = new CategoryController(mock.Object);

        // Act
        var result = await controller.UpdateCategory(null!);

        // Assert
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task UpdateCategory_ShouldReturnBadRequest_WhenModelStateIsInvalid()
    {
        // Arrange
        var mock = new Mock<ITondForooshRepository>();
        var controller = new CategoryController(mock.Object);
        controller.ModelState.AddModelError("error", "some error");
        var updateDto = new UpdateCategoryDto { Id = 1 };

        // Act
        var result = await controller.UpdateCategory(updateDto);

        // Assert
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task UpdateCategory_ShouldNotModifyCategory_WhenNameIsEmpty()
    {
        // Arrange
        var category = new Category { Id = 1, Name = "Original Name" };
        var mock = new Mock<ITondForooshRepository>();
        mock.Setup(m => m.Categories).Returns(new List<Category> { category }.AsQueryable().BuildMock());
        mock.Setup(m => m.UpdateCategoryAsync(It.IsAny<Category>())).Returns(Task.CompletedTask);
        var controller = new CategoryController(mock.Object);
        var updateDto = new UpdateCategoryDto { Id = 1, Name = "" };

        // Act
        var result = await controller.UpdateCategory(updateDto);

        // Assert
        Assert.IsType<NoContentResult>(result);
        Assert.Equal("Original Name", category.Name);
    }
    #endregion
}