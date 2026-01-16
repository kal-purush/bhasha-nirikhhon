using CarService.AppCore.Interfaces;
using CarService.Domain.Models;
using CarService.EventProcessor.Cqrs.Commands;
using CarService.EventProcessor.Cqrs.Commands.Handlers;
using Moq;
using System;
using System.Threading.Tasks;
using Xunit;

namespace CarService.EventProcessor.Tests.Cqrs
{
    public class CreateRepairOrderCommandHandlerTest
    {
        private readonly Mock<IRepairOrdersRepository> _repository;
        private readonly CreateRepairOrderCommandHandler _handler;

        public CreateRepairOrderCommandHandlerTest()
        {
            _repository = new Mock<IRepairOrdersRepository>();
            _handler = new CreateRepairOrderCommandHandler(_repository.Object);
        }

        [Fact]
        public async Task Handler_Should_Handle_CreateRepairOrderCommand_And_Return_CreatedRepairOrder()
        {
            // Arrange
            var command = new CreateRepairOrderCommand
            {
                Id = Guid.Empty,
                CarId = Guid.Empty,
                OrderDate = DateTime.MinValue,
                Price = 0
            };

            var repairOrder = new RepairOrder
            {
                Id = Guid.Empty,
                CarId = Guid.Empty,
                OrderDate = DateTime.MinValue,
                Price = 0
            };

            _repository.Setup(r => r.Create(It.IsAny<RepairOrder>())).ReturnsAsync(repairOrder);

            // Act
            var result = await _handler.Handle(command, default);

            // Assert
            Assert.True(result.Id == repairOrder.Id && result.CarId == repairOrder.CarId
                && result.OrderDate == repairOrder.OrderDate && result.Price == repairOrder.Price);
        }
    }
}