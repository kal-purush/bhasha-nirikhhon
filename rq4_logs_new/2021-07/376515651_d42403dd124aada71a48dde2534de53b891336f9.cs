using CarService.AppCore.Interfaces;
using CarService.Cqrs.Commands;
using CarService.Cqrs.Commands.Handlers;
using CarService.Domain.Models;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace CarService.Cqrs.Tests
{
    public class CreateCarCommandHandlerTest
    {
        private readonly Mock<ICarsRepository> _repository;
        private readonly CreateCarCommandHandler _handler;

        public CreateCarCommandHandlerTest()
        {
            _repository = new Mock<ICarsRepository>();
            _handler = new CreateCarCommandHandler(_repository.Object);
        }

        [Fact]
        public async Task Handler_Should_Handle_CreateCarCommand_And_Return_CreatedCar()
        {
            // Arrange
            var car = new Car
            {
                Id = default,
                Make = default,
                Millage = 0,
                Model = default,
                OwnerId = default,
                Vin = default,
                Year = default
            };

            var command = new CreateCarCommand(car);

            _repository.Setup(r => r.Create(It.IsAny<Car>())).ReturnsAsync(car);

            // Act
            var model = await _handler.Handle(command, default);

            // Assert
            Assert.Equal(car, model);
        }
    }
}