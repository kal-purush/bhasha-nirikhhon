using Xunit;
using Moq;
using AutoMapper;
using SecretCode.Tests.Global;
using Model = SecretCode.Api.Models;
using SecretCode.Api.Features.User.Handlers;
using SecretCode.Api.Features.User.Commands;

namespace SecretCode.Tests.Features.User;

[Collection("Fixture Collection")]
public class CreateUserHandlerTests
{
    GlobalFixture _fixture;
    public CreateUserHandlerTests(GlobalFixture fixture)
    {
        _fixture = fixture;
    }   
    
    [Fact]
    public async Task Handle_ShouldAddUser_WhenValidationPasses()
    {
        var initialCount = _fixture._users.Count;
        //Arrange
        _fixture._contextMock.Setup(_ => _.Users.AddAsync(It.IsAny<Model.User>(), It.IsAny<CancellationToken>()))
            .Callback((Model.User user, CancellationToken token) => 
            { 
                _fixture._users.Add(user);
            });

        _fixture._mapperMock.Setup(_ => _.Map<Model.User>(It.IsAny<CreateUserCommand>()))
                .Returns(new Model.User
                {
                    Email = "newEmail@testEmail.com",
                    Name = "New Name",
                    DateCreated = DateTime.UtcNow,
                    DateModified = DateTime.UtcNow,
                    Deleted = false
                });
        //Act
        var handler = new CreateUserHandler(_fixture._contextMock.Object, _fixture._mapperMock.Object);
        CreateUserCommand command = new CreateUserCommand
        {
            Email = "newEmail@testEmail.com",
            Name = "New Name"
        };
        
        //Assert
        await handler.Handle(command, default);
        Assert.True(_fixture._users.Count == initialCount + 1);
        Assert.NotNull(_fixture._users.SingleOrDefault(_ => _.Name == command.Name));
        Assert.Equal(_fixture._users.SingleOrDefault(_ => _.Name == command.Name)?.Name, command.Name);
        Assert.Equal(_fixture._users.SingleOrDefault(_ => _.Name == command.Name)?.Email, command.Email);
        _fixture._contextMock.Verify(_ =>  _.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}