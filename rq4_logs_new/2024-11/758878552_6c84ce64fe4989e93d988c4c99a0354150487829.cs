using AutoFixture.Xunit2;
using Jared.Application.Controllers;
using Jared.Application.Requests.Roles.List;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.Role;
using MediatR;
using Moq;

namespace Jared.Application.Tests.Controllers;

public class RoleControllerTest
{
    private readonly RoleController controller;
    private readonly Mock<IMediator> mediatorMock = new();

    public RoleControllerTest()
    {
        controller = new(mediatorMock.Object);
    }

    #region RoleListAsync
    [Theory]
    [AutoData]
    public async Task RoleListAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(List<RoleListDto> dtos)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<RoleListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dtos)));

        // Act
        var result = await controller.RoleListAsync();

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dtos, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new RoleListQuery(), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task RoleListAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<RoleListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<List<RoleListDto>>(errorMessage)));

        // Act
        var result = await controller.RoleListAsync();

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new RoleListQuery(), default), Times.Once);
    }
    #endregion
}