using AutoFixture.Xunit2;
using Jared.Application.Controllers;
using Jared.Application.Requests.Projects.Create;
using Jared.Application.Requests.Projects.Details;
using Jared.Application.Requests.Projects.List;
using Jared.Application.Requests.Projects.Page;
using Jared.Application.Requests.Projects.Update;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.ProjectDtos;
using Jared.Shared.Enums;
using MediatR;
using Moq;

namespace Jared.Application.Tests.Controllers;

public class ProjectControllerTest
{
    private readonly ProjectController controller;
    private readonly Mock<IMediator> mediatorMock = new();

    public ProjectControllerTest()
    {
        controller = new(mediatorMock.Object);
    }

    #region ProjectDetailsAsync
    [Theory]
    [AutoData]
    public async Task ProjectDetailsAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(ProjectDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectDetailsQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dto)));

        // Act
        var result = await controller.ProjectDetailsAsync(dto.Id);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dto, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectDetailsQuery(dto.Id), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task ProjectDetailsAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectDetailsQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<ProjectDetailsDto>(errorMessage)));

        // Act
        var result = await controller.ProjectDetailsAsync(1);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectDetailsQuery(1), default), Times.Once);
    }
    #endregion

    #region ProjectListAsync
    [Theory]
    [AutoData]
    public async Task ProjectListAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(List<ProjectListDto> dtos)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dtos)));

        // Act
        var result = await controller.ProjectListAsync();

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dtos, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectListQuery(), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task ProjectListAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<List<ProjectListDto>>(errorMessage)));

        // Act
        var result = await controller.ProjectListAsync();

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectListQuery(), default), Times.Once);
    }
    #endregion

    #region ProjectPageAsync
    [Theory]
    [AutoData]
    public async Task ProjectPageAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(
        ProjectPageDto dto,
        int page,
        int pageSize,
        string sortingProperty,
        SortingDirection sortingDirection,
        IDictionary<string, string?>? filter)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectPageQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dto)));

        // Act
        var result = await controller.ProjectPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dto, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(
            new ProjectPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task ProjectPageAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        string errorMessage,
        int page,
        int pageSize,
        string sortingProperty,
        SortingDirection sortingDirection,
        IDictionary<string, string?>? filter)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectPageQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<ProjectPageDto>(errorMessage)));

        // Act
        var result = await controller.ProjectPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(
            new ProjectPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
    }
    #endregion

    #region ProjectUpdateAsync
    [Theory]
    [AutoData]
    public async Task ProjectUpdateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(ProjectDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.ProjectUpdateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectUpdateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task ProjectUpdateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        ProjectDetailsDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.ProjectUpdateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectUpdateCommand(dto), default), Times.Once);
    }
    #endregion

    #region ProjectCreateAsync
    [Theory]
    [AutoData]
    public async Task ProjectCreateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(ProjectDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectCreateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.ProjectCreateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectCreateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task ProjectCreateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        ProjectDetailsDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<ProjectCreateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.ProjectCreateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new ProjectCreateCommand(dto), default), Times.Once);
    }
    #endregion
}