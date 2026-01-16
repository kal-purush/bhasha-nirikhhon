using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;


namespace EasyTravel.Tests
{
    [TestFixture]
    public class BusServiceTests
    {
        private Mock<IApplicationUnitOfWork> _mockUnitOfWork;
        private Mock<IBusRepository> _mockBusRepository;
        private BusService _busService;

        [SetUp]
        public void SetUp()
        {
            _mockUnitOfWork = new Mock<IApplicationUnitOfWork>();
            _mockBusRepository = new Mock<IBusRepository>();
            _mockUnitOfWork.Setup(u => u.BusRepository).Returns(_mockBusRepository.Object);
            _busService = new BusService(_mockUnitOfWork.Object);
        }

        [Test]
        public void CreateBus_ShouldAddBusWithSeats()
        {
            // Arrange
            var bus = new Bus
            {
                Id = Guid.NewGuid(),
                OperatorName = "Test Operator",
                From = "City A",
                To = "City B",
                DepartureTime = DateTime.Now,
                ArrivalTime = DateTime.Now.AddHours(2),
                Price = 100,
                TotalSeats = 28,
                Seats = new List<Seat>()
            };

            // Act
            _busService.CreateBus(bus);

            // Assert
            _mockBusRepository.Verify(r => r.Addbus(bus), Times.Once);
            _mockUnitOfWork.Verify(u => u.Save(), Times.Once);
            Assert.That(bus.Seats.Count, Is.EqualTo(28));
        }

        [Test]
        public void GetAllBuses_ShouldReturnAllBuses()
        {
            // Arrange
            var buses = new List<Bus>
            {
                new Bus
                {
                    Id = Guid.NewGuid(),
                    OperatorName = "Operator 1",
                    From = "City A",
                    To = "City B",
                    DepartureTime = DateTime.Now,
                    ArrivalTime = DateTime.Now.AddHours(2),
                    Price = 100,
                    TotalSeats = 28,
                    Seats = new List<Seat>()
                },
                new Bus
                {
                    Id = Guid.NewGuid(),
                    OperatorName = "Operator 2",
                    From = "City C",
                    To = "City D",
                    DepartureTime = DateTime.Now,
                    ArrivalTime = DateTime.Now.AddHours(2),
                    Price = 150,
                    TotalSeats = 28,
                    Seats = new List<Seat>()
                }
            };
            _mockBusRepository.Setup(r => r.GetAllBuses()).Returns(buses);

            // Act
            var result = _busService.GetAllBuses();

            // Assert
            
            Assert.That(result.Count(), Is.EqualTo(2));
        }

        [Test]
        public void GetBusById_ShouldReturnBus()
        {
            // Arrange
            var busId = Guid.NewGuid();
            var bus = new Bus
            {
                Id = busId,
                OperatorName = "Test Operator",
                From = "City A",
                To = "City B",
                DepartureTime = DateTime.Now,
                ArrivalTime = DateTime.Now.AddHours(2),
                Price = 100,
                TotalSeats = 28,
                Seats = new List<Seat>()
            };
            _mockBusRepository.Setup(r => r.GetById(busId)).Returns(bus);

            // Act
            var result = _busService.GetBusById(busId);

            // Assert
            
            Assert.That(busId, Is.EqualTo(result.Id));

        }

        [Test]
        public async Task GetAvailableBusesAsync_ShouldReturnAvailableBuses()
        {
            // Arrange
            var from = "City A";
            var to = "City B";
            var dateTime = DateTime.Now;
            var buses = new List<Bus>
 {
     new Bus
     {
         Id = Guid.NewGuid(),
         OperatorName = "Operator 1",
         From = "City A",
         To = "City B",
         DepartureTime = dateTime,
         ArrivalTime = dateTime.AddHours(2),
         Price = 100,
         TotalSeats = 28,
         Seats = new List<Seat>()
     },
     new Bus
     {
         Id = Guid.NewGuid(),
         OperatorName = "Operator 2",
         From = "City A",
         To = "City B",
         DepartureTime = dateTime,
         ArrivalTime = dateTime.AddHours(2),
         Price = 150,
         TotalSeats = 28,
         Seats = new List<Seat>()
     }
 };
            _mockBusRepository.Setup(r => r.GetAsync(It.IsAny<Expression<Func<Bus, bool>>>(), It.IsAny<Func<IQueryable<Bus>, IIncludableQueryable<Bus, object>>>())).ReturnsAsync(buses);

            // Act
            var result = await _busService.GetAvailableBusesAsync(from, to, dateTime);

            // Assert
            Assert.That(result.Count, Is.EqualTo(2));

        }

        [Test]
        public void UpdateBus_ShouldEditBus()
        {
            // Arrange
            var bus = new Bus
            {
                Id = Guid.NewGuid(),
                OperatorName = "Test Operator",
                From = "City A",
                To = "City B",
                DepartureTime = DateTime.Now,
                ArrivalTime = DateTime.Now.AddHours(2),
                Price = 100,
                TotalSeats = 28,
                Seats = new List<Seat>()
            };

            // Act
            _busService.UpdateBus(bus);

            // Assert
            _mockBusRepository.Verify(r => r.Edit(bus), Times.Once);
            _mockUnitOfWork.Verify(u => u.Save(), Times.Once);
        }

        [Test]
        public void DeleteBus_ShouldRemoveBus()
        {
            // Arrange
            var bus = new Bus
            {
                Id = Guid.NewGuid(),
                OperatorName = "Test Operator",
                From = "City A",
                To = "City B",
                DepartureTime = DateTime.Now,
                ArrivalTime = DateTime.Now.AddHours(2),
                Price = 100,
                TotalSeats = 28,
                Seats = new List<Seat>()
            };

            // Act
            _busService.DeleteBus(bus);

            // Assert
            _mockBusRepository.Verify(r => r.Remove(bus), Times.Once);
            _mockUnitOfWork.Verify(u => u.Save(), Times.Once);
        }

       


    }
}