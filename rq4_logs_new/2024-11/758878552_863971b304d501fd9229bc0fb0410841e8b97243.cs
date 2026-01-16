using AutoFixture.Xunit2;
using Jared.Application.Controllers;
using Jared.Application.Requests.Epics.Create;
using Jared.Application.Requests.Epics.Details;
using Jared.Application.Requests.Epics.List;
using Jared.Application.Requests.Epics.Page;
using Jared.Application.Requests.Epics.Update;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.EpicDtos;
using Jared.Shared.Enums;
using MediatR;
using Moq;

namespace Jared.Application.Tests.Controllers;

public class EpicControllerTest
{
    private readonly EpicController controller;
    private readonly Mock<IMediator> mediatorMock = new();

    public EpicControllerTest()
    {
        controller = new(mediatorMock.Object);
    }

    #region EpicDetailsAsync
    [Theory]
    [AutoData]
    public async Task EpicDetailsAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(EpicDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicDetailsQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dto)));

        // Act
        var result = await controller.EpicDetailsAsync(dto.Id);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dto, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicDetailsQuery(dto.Id), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task EpicDetailsAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicDetailsQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<EpicDetailsDto>(errorMessage)));

        // Act
        var result = await controller.EpicDetailsAsync(1);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicDetailsQuery(1), default), Times.Once);
    }
    #endregion

    #region EpicListAsync
    [Theory]
    [AutoData]
    public async Task EpicListAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(List<EpicListDto> dtos)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dtos)));

        // Act
        var result = await controller.EpicListAsync(null);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dtos, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicListQuery(null), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task EpicListAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<List<EpicListDto>>(errorMessage)));

        // Act
        var result = await controller.EpicListAsync(null);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicListQuery(null), default), Times.Once);
    }
    #endregion

    #region EpicPageAsync
    [Theory]
    [AutoData]
    public async Task EpicPageAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(
        EpicPageDto dto,
        int page,
        int pageSize,
        string sortingProperty,
        SortingDirection sortingDirection,
        IDictionary<string, string?>? filter)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicPageQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dto)));

        // Act
        var result = await controller.EpicPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dto, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(
            new EpicPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task EpicPageAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        string errorMessage,
        int page,
        int pageSize,
        string sortingProperty,
        SortingDirection sortingDirection,
        IDictionary<string, string?>? filter)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicPageQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<EpicPageDto>(errorMessage)));

        // Act
        var result = await controller.EpicPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(
            new EpicPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
    }
    #endregion

    #region EpicUpdateAsync
    [Theory]
    [AutoData]
    public async Task EpicUpdateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(EpicDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.EpicUpdateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicUpdateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task EpicUpdateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        EpicDetailsDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.EpicUpdateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicUpdateCommand(dto), default), Times.Once);
    }
    #endregion

    #region EpicCreateAsync
    [Theory]
    [AutoData]
    public async Task EpicCreateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(EpicDetailsDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicCreateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.EpicCreateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicCreateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task EpicCreateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        EpicDetailsDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<EpicCreateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.EpicCreateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new EpicCreateCommand(dto), default), Times.Once);
    }
    #endregion
}