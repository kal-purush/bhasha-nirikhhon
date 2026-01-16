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
    public class AdminGuideBookingServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock;
        private Mock<ILogger<AdminGuideBookingService>> _loggerMock;
        private AdminGuideBookingService _adminGuideBookingService;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<AdminGuideBookingService>>();
            _adminGuideBookingService = new AdminGuideBookingService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void Get_ShouldReturnGuideBooking_WhenGuideBookingExists()
        {
            // Arrange
            var guideBookingId = Guid.NewGuid();
            var guideBooking = new GuideBooking
            {
                Id = guideBookingId,
                UserName = "John Doe",
                Email = "johndoe@example.com",
                PhoneNumber = "123-456-7890",
                Gender = "Male",
                EventType = "City Tour",
                EventLocation = "New York",
                EventDate = DateTime.Now,
                StartTime = TimeSpan.FromHours(9),
                EndTime = TimeSpan.FromHours(12),
                TimeInHour = 3,
                GuideId = Guid.NewGuid()
            };
            _unitOfWorkMock.Setup(u => u.GuideBookingRepository.GetById(guideBookingId)).Returns(guideBooking);

            // Act
            var result = _adminGuideBookingService.Get(guideBookingId);

            // Assert
            Assert.That(result, Is.EqualTo(guideBooking), "The returned guide booking should match the expected guide booking.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Fetching guide booking with ID: {guideBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Get_ShouldThrowKeyNotFoundException_WhenGuideBookingDoesNotExist()
        {
            // Arrange
            var guideBookingId = Guid.NewGuid();
            _unitOfWorkMock.Setup(u => u.GuideBookingRepository.GetById(guideBookingId)).Returns((GuideBooking)null);

            // Act
            var ex = Assert.Throws<InvalidOperationException>(() => _adminGuideBookingService.Get(guideBookingId));

            // Assert
            Assert.That(ex, Is.Not.Null, "An exception should be thrown.");
            Assert.That(ex.Message, Is.EqualTo($"An error occurred while fetching the guide booking with ID: {guideBookingId}."));
            Assert.That(ex.InnerException, Is.TypeOf<KeyNotFoundException>(), "The inner exception should be a KeyNotFoundException.");
            Assert.That(ex.InnerException?.Message, Is.EqualTo($"Guide booking with ID: {guideBookingId} not found."));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Guide booking with ID: {guideBookingId} not found.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"An error occurred while fetching the guide booking with ID: {guideBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void GetAll_ShouldReturnAllGuideBookings()
        {
            // Arrange
            var guideBookings = new List<GuideBooking>
            {
                new GuideBooking
                {
                    Id = Guid.NewGuid(),
                    UserName = "John Doe",
                    Email = "johndoe@example.com",
                    PhoneNumber = "123-456-7890",
                    Gender = "Male",
                    EventType = "City Tour",
                    EventLocation = "New York",
                    EventDate = DateTime.Now,
                    StartTime = TimeSpan.FromHours(9),
                    EndTime = TimeSpan.FromHours(12),
                    TimeInHour = 3,
                    GuideId = Guid.NewGuid()
                },
                new GuideBooking
                {
                    Id = Guid.NewGuid(),
                    UserName = "Jane Smith",
                    Email = "janesmith@example.com",
                    PhoneNumber = "987-654-3210",
                    Gender = "Female",
                    EventType = "Museum Tour",
                    EventLocation = "Los Angeles",
                    EventDate = DateTime.Now,
                    StartTime = TimeSpan.FromHours(10),
                    EndTime = TimeSpan.FromHours(14),
                    TimeInHour = 4,
                    GuideId = Guid.NewGuid()
                }
            };
            _unitOfWorkMock.Setup(u => u.GuideBookingRepository.GetAll()).Returns(guideBookings);

            // Act
            var result = _adminGuideBookingService.GetAll();

            // Assert
            Assert.That(result, Is.EqualTo(guideBookings), "The returned guide bookings should match the expected list.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains("Fetching all guide bookings.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Delete_ShouldRemoveGuideBookingAndSave()
        {
            // Arrange
            var guideBookingId = Guid.NewGuid();
            var bookingRepositoryMock = new Mock<IBookingRepository>();
            _unitOfWorkMock.Setup(u => u.BookingRepository).Returns(bookingRepositoryMock.Object);

            // Act
            _adminGuideBookingService.Delete(guideBookingId);

            // Assert
            bookingRepositoryMock.Verify(r => r.Remove(guideBookingId), Times.Once, "Guide booking should be removed from the repository.");
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once, "Save should be called after removing the guide booking.");
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Attempting to delete guide booking with ID: {guideBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains($"Successfully deleted guide booking with ID: {guideBookingId}")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }

        [Test]
        public void Delete_ShouldThrowArgumentException_WhenIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _adminGuideBookingService.Delete(Guid.Empty));
            Assert.That(ex.ParamName, Is.EqualTo("id"));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v != null && v.ToString().Contains("Invalid guide booking ID provided for deletion.")),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
                Times.Once);
        }
 
    }
}