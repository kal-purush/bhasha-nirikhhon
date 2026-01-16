using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace EasyTravel.Test.ApplicationsTests.ServiceTests
{
    [TestFixture]
    public class AdminHotelBookingServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock;
        private Mock<ILogger<AdminHotelBookingService>> _loggerMock;
        private AdminHotelBookingService _adminHotelBookingService;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<AdminHotelBookingService>>();
            _adminHotelBookingService = new AdminHotelBookingService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void Get_ShouldReturnHotelBooking_WhenHotelBookingExists()
        {
            // Arrange
            var hotelBookingId = Guid.NewGuid();
            var hotelBooking = new HotelBooking
            {
                Id = hotelBookingId,
                HotelId = Guid.NewGuid(),
                CheckInDate = DateTime.Now,
                CheckOutDate = DateTime.Now.AddDays(2),
                RoomIdsJson = "[\"Room1\", \"Room2\"]"
            };
            _unitOfWorkMock.Setup(u => u.HotelBookingRepository.GetById(hotelBookingId)).Returns(hotelBooking);

            // Act
            var result = _adminHotelBookingService.Get(hotelBookingId);

            // Assert
            Assert.That(result, Is.EqualTo(hotelBooking), "The returned hotel booking should match the expected hotel booking.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Fetching hotel booking with ID: {hotelBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Get_ShouldThrowKeyNotFoundException_WhenHotelBookingDoesNotExist()
        {
            // Arrange
            var hotelBookingId = Guid.NewGuid();
            _unitOfWorkMock.Setup(u => u.HotelBookingRepository.GetById(hotelBookingId)).Returns((HotelBooking)null);

            // Act
            var ex = Assert.Throws<InvalidOperationException>(() => _adminHotelBookingService.Get(hotelBookingId));

            // Assert
            Assert.That(ex, Is.Not.Null, "An exception should be thrown.");
            Assert.That(ex.Message, Is.EqualTo($"An error occurred while fetching the hotel booking with ID: {hotelBookingId}."));
            Assert.That(ex.InnerException, Is.TypeOf<KeyNotFoundException>(), "The inner exception should be a KeyNotFoundException.");
            Assert.That(ex.InnerException?.Message, Is.EqualTo($"Hotel booking with ID: {hotelBookingId} not found."));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Hotel booking with ID: {hotelBookingId} not found.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"An error occurred while fetching the hotel booking with ID: {hotelBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void GetAll_ShouldReturnAllHotelBookings()
        {
            // Arrange
            var hotelBookings = new List<HotelBooking>
            {
                new HotelBooking
                {
                    Id = Guid.NewGuid(),
                    HotelId = Guid.NewGuid(),
                    CheckInDate = DateTime.Now,
                    CheckOutDate = DateTime.Now.AddDays(2),
                    RoomIdsJson = "[\"Room1\", \"Room2\"]"
                },
                new HotelBooking
                {
                    Id = Guid.NewGuid(),
                    HotelId = Guid.NewGuid(),
                    CheckInDate = DateTime.Now,
                    CheckOutDate = DateTime.Now.AddDays(3),
                    RoomIdsJson = "[\"Room3\", \"Room4\"]"
                }
            };
            _unitOfWorkMock.Setup(u => u.HotelBookingRepository.GetAll()).Returns(hotelBookings);

            // Act
            var result = _adminHotelBookingService.GetAll();

            // Assert
            Assert.That(result, Is.EqualTo(hotelBookings), "The returned hotel bookings should match the expected list.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains("Fetching all hotel bookings.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Delete_ShouldRemoveHotelBookingAndSave()
        {
            // Arrange
            var hotelBookingId = Guid.NewGuid();
            var bookingRepositoryMock = new Mock<IBookingRepository>();
            _unitOfWorkMock.Setup(u => u.BookingRepository).Returns(bookingRepositoryMock.Object);

            // Act
            _adminHotelBookingService.Delete(hotelBookingId);

            // Assert
            bookingRepositoryMock.Verify(r => r.Remove(hotelBookingId), Times.Once, "Hotel booking should be removed from the repository.");
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once, "Save should be called after removing the hotel booking.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Attempting to delete hotel booking with ID: {hotelBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Successfully deleted hotel booking with ID: {hotelBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Delete_ShouldThrowArgumentException_WhenIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _adminHotelBookingService.Delete(Guid.Empty));
            Assert.That(ex.ParamName, Is.EqualTo("id"));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains("Invalid hotel booking ID provided for deletion.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }
    }
}



