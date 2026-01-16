using CarService.Users.Api.Cqrs.Commands;
using CarService.Users.Api.Cqrs.Commands.Handlers;
using CarService.Users.Api.Interfaces;
using CarService.Users.Api.Models.Domain;
using Moq;
using System;
using System.Threading.Tasks;
using Xunit;

namespace CarService.Users.Api.Tests.Cqrs.Commands
{
    public class CreateUserCommandHandlerTest
    {
        private readonly Mock<IUsersRepository> _usersRepository;
        private readonly CreateUserCommandHandler _handler;

        public CreateUserCommandHandlerTest()
        {
            _usersRepository = new Mock<IUsersRepository>();
            _handler = new CreateUserCommandHandler(_usersRepository.Object);
        }

        [Fact]
        public async Task CreateUserCommandHandler_Should_Handle_Command_And_Return_UserDto()
        {
            // Arrange
            var command = new CreateUserCommand("Test", "TestLastName", DateTime.MinValue, Guid.NewGuid());

            // Act
            var result = await _handler.Handle(command, default);
            
            // Assert
            Assert.True(result.CarId == command.CarId && result.DoB == command.DoB
                && result.FirstName == command.FirstName && result.LastName == command.LastName);
        }
    }
}