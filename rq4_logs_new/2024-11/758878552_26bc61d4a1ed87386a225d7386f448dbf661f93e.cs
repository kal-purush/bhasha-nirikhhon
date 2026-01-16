using AutoFixture.Xunit2;
using Jared.Application.Controllers;
using Jared.Application.Requests.Users.List;
using Jared.Application.Requests.Users.Login;
using Jared.Application.Requests.Users.Password;
using Jared.Application.Requests.Users.Register;
using Jared.Application.Requests.Users.Update;
using Jared.Application.Requests.Users.UpdateRole;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.UserDtos;
using MediatR;
using Moq;

namespace Jared.Application.Tests.Controllers;

public class UserControllerTest
{
    private readonly UserController controller;
    private readonly Mock<IMediator> mediatorMock = new();

    public UserControllerTest()
    {
        controller = new(mediatorMock.Object);
    }

    #region UserListAsync
    [Theory]
    [AutoData]
    public async Task UserListAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(List<UserListDto> dtos)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(dtos)));

        // Act
        var result = await controller.UserListAsync();

        // Assert
        Assert.True(result.Success);
        Assert.Equal(dtos, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserListQuery(), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserListAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserListQuery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<List<UserListDto>>(errorMessage)));

        // Act
        var result = await controller.UserListAsync();

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserListQuery(), default), Times.Once);
    }
    #endregion

    #region UserRegisterAsync
    [Theory]
    [AutoData]
    public async Task UserRegisterAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(UserRegisterDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserRegisterCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.UserRegisterAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserRegisterCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserRegisterAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        UserRegisterDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserRegisterCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.UserRegisterAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserRegisterCommand(dto), default), Times.Once);
    }
    #endregion

    #region UserLoginAsync
    [Theory]
    [AutoData]
    public async Task UserLoginAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(UserLoginDto dto, string token)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserLoginCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(token)));

        // Act
        var result = await controller.UserLoginAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(token, result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserLoginCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserLoginAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        UserLoginDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserLoginCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<string>(errorMessage)));

        // Act
        var result = await controller.UserLoginAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.Null(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserLoginCommand(dto), default), Times.Once);
    }
    #endregion

    #region UserPasswordAsync
    [Theory]
    [AutoData]
    public async Task UserPasswordAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(UserPasswordDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserPasswordCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.UserPasswordAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserPasswordCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserPasswordAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        UserPasswordDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserPasswordCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.UserPasswordAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserPasswordCommand(dto), default), Times.Once);
    }
    #endregion

    #region UserUpdateAsync
    [Theory]
    [AutoData]
    public async Task UserUpdateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(UserUpdateDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.UserUpdateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserUpdateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserUpdateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        UserUpdateDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.UserUpdateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserUpdateCommand(dto), default), Times.Once);
    }
    #endregion

    #region UserRoleUpdateAsync
    [Theory]
    [AutoData]
    public async Task UserRoleUpdateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(UserRoleUpdateDto dto)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserRoleUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Ok(true)));

        // Act
        var result = await controller.UserRoleUpdateAsync(dto);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.Data);
        Assert.Equal(string.Empty, result.Error);
        mediatorMock.Verify(x => x.Send(new UserRoleUpdateCommand(dto), default), Times.Once);
    }

    [Theory]
    [AutoData]
    public async Task UserRoleUpdateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
        UserRoleUpdateDto dto,
        string errorMessage)
    {
        // Arrange
        mediatorMock.Setup(x => x.Send(It.IsAny<UserRoleUpdateCommand>(), It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

        // Act
        var result = await controller.UserRoleUpdateAsync(dto);

        // Assert
        Assert.False(result.Success);
        Assert.False(result.Data);
        Assert.Equal(errorMessage, result.Error);
        mediatorMock.Verify(x => x.Send(new UserRoleUpdateCommand(dto), default), Times.Once);
    }
    #endregion
}