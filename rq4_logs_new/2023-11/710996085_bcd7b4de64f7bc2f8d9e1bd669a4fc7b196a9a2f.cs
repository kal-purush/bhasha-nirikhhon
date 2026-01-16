using FluentAssertions;
using SecretCode.Tests.Global;
using FluentValidation.TestHelper;
using Xunit;
using SecretCode.Api.Features.User.Validators;
using SecretCode.Api.Features.User.Commands;
using Models = SecretCode.Api.Models;
using SecretCode.Api.Features.User.Handlers;


namespace SecretCode.Tests.Features.User;

[Collection("Fixture Collection")]
public class EditUserHandlerTests
{
    private readonly GlobalFixture _fixture;
    public EditUserHandlerTests(GlobalFixture fixture) => _fixture = fixture;

    [Fact]
    public void Handle_ShouldNot_EditUser_WhenEmailIsNotProvided()
    {
        //Arrange
        var user = _fixture._users.ElementAt(new Random().Next(_fixture._users.Count));

        var validator = new EditUserCommandValidator();
        var editCommand = new EditUserCommand
        {
            Deleted = user.Deleted,
            Email = null,
            Name = user.Name,
        };

        //act
        var result = validator.TestValidate(editCommand);
        
        //Assert
        result.ShouldHaveValidationErrorFor(_ => _.Email).Only();
    }

    [Fact]
    public void Handle_ShouldNot_EditUser_WhenNameIsNotProvided()
    {
        //Arrange
        var user = _fixture._users.ElementAt(new Random().Next(_fixture._users.Count));

        var validator = new EditUserCommandValidator();
        var editCommand = new EditUserCommand
        {
            Deleted = user.Deleted,
            Email = user.Email,
            Name = null,
        };

        //act
        var result = validator.TestValidate(editCommand);
        
        //Assert
        result.ShouldHaveValidationErrorFor(_ => _.Name).Only();
    }

    [Fact]
    public async Task Handle_Should_EditUser_WhenValidationPasses()
    {
        //Arrange
        var user = _fixture._users.ElementAt(new Random().Next(_fixture._users.Count));
        
        var validator = new EditUserCommandValidator();
        var command = new EditUserCommand
        {
            Deleted = !user.Deleted,
            Email = "Updated Email",
            Name = "Updated Name",
            Id = user.Id
        };

        var handler = new EditUserHandler(_fixture._contextMock.Object);

        _fixture._contextMock.Setup(_ => _.SaveChangesAsync(default))
                        .Callback((CancellationToken token) => 
                        {
                            var userToUpdate = _fixture._users.SingleOrDefault(_ => _.Id == user.Id);
                            
                            userToUpdate.Name = user.Name;
                            userToUpdate.Email = user.Email;
                            userToUpdate.Deleted = user.Deleted;
                        });

        //act
        await handler.Handle(command, default);
        
        //Assert
        _fixture._users.SingleOrDefault(_ => _.Id == command.Id).Should().NotBeNull();
        _fixture._users.SingleOrDefault(_ => _.Id == command.Id)?.Name.Should().Be(command.Name);
        _fixture._users.SingleOrDefault(_ => _.Id == command.Id)?.Email.Should().Be(command.Email);
        _fixture._users.SingleOrDefault(_ => _.Id == command.Id)?.Deleted.Should().Be(command.Deleted);
    }
}