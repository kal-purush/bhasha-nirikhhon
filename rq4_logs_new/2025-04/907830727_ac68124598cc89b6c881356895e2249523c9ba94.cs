using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using EasyTravel.Domain.Services;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace EasyTravel.Test.ApplicationsTests.ServiceTests
{
    [TestFixture]
    public class AdminBusBookingServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock;
        private Mock<ILogger<AdminBusBookingService>> _loggerMock;
        private AdminBusBookingService _adminBusBookingService;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<AdminBusBookingService>>();
            _adminBusBookingService = new AdminBusBookingService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void DeleteBusBooking_ShouldDeleteBusBookingAndSave()
        {
            // Arrange
            var busBookingId = Guid.NewGuid();
            var busBookingRepositoryMock = new Mock<IBusBookingRepository>();
            _unitOfWorkMock.Setup(u => u.BusBookingRepository).Returns(busBookingRepositoryMock.Object);

            // Act
            _adminBusBookingService.DeleteBusBooking(busBookingId);

            // Assert
            busBookingRepositoryMock.Verify(r => r.DeleteBusBooking(busBookingId), Times.Once, "Bus booking should be deleted from the repository.");
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once, "Save should be called after deleting the bus booking.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Attempting to delete bus booking with Id: {busBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Successfully deleted bus booking with Id: {busBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void GetAllBusBookings_ShouldReturnAllBusBookings()
        {
            // Arrange
            IEnumerable<BusBooking> busBookings = new List<BusBooking>
            {
                new BusBooking
                {
                    Id = Guid.NewGuid(),
                    BusId = Guid.NewGuid(),
                    PassengerName = "John Doe",
                    Email = "johndoe@example.com",
                    PhoneNumber = "123-456-7890",
                    BookingDate = DateTime.Now,
                    SelectedSeats = new List<string> { "A1", "A2" },
                    SelectedSeatIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() }
                },
                new BusBooking
                {
                    Id = Guid.NewGuid(),
                    BusId = Guid.NewGuid(),
                    PassengerName = "Jane Smith",
                    Email = "janesmith@example.com",
                    PhoneNumber = "987-654-3210",
                    BookingDate = DateTime.Now,
                    SelectedSeats = new List<string> { "B1", "B2" },
                    SelectedSeatIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() }
                }
            };
            var busBookingRepositoryMock = new Mock<IBusBookingRepository>();
            _unitOfWorkMock.Setup(u => u.BusBookingRepository).Returns(busBookingRepositoryMock.Object);
            busBookingRepositoryMock.Setup(r => r.GetAllBusBookings()).Returns(busBookings);

            // Act
            var result = _adminBusBookingService.GetAllBusBookings();

            // Assert
            Assert.That(result, Is.EqualTo(busBookings), "The returned bus bookings should match the expected list.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains("Fetching all bus bookings.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }
    }
}
