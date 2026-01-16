using Moq;
using FluentAssertions;
using Xunit;
using SecretCode.Tests.Global;
using SecretCode.Api.Features.User.Commands;
using SecretCode.Api;


namespace SecretCode.Tests.Features.User;

[Collection("Fixture Collection")]
public class DeleteUserHandlerTests
{
    private readonly GlobalFixture _fixture;

    public DeleteUserHandlerTests(GlobalFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Handle_ShouldDeleteUser_WhenExecuted()
    {
        //Arrange
        var id = _fixture._users.ElementAt( new Random().Next( _fixture._users.Count ) ).Id;
        var command = new DeleteUserCommand { Id = id };
        var handler = new DeleteUserHandler(_fixture._contextMock.Object);
        var initialCount = _fixture._users.Count;
        //Act
        await handler.Handle(command, default);

        //Assert
        _fixture._users.Count.Should().Be(--initialCount);
        _fixture._users.FirstOrDefault(_ => _.Id == id).Should().BeNull();
    }
}