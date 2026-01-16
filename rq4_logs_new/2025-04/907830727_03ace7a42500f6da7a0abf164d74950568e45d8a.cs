using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace EasyTravel.Test.ApplicationTests.Services
{
    [TestFixture]
    public class PhotographerBookingServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock = null!;
        private Mock<ILogger<PhotographerBookingService>> _loggerMock = null!;
        private PhotographerBookingService _photographerBookingService = null!;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<PhotographerBookingService>>();
            _photographerBookingService = new PhotographerBookingService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void SaveBooking_ShouldThrowArgumentNullException_WhenModelIsNull()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _photographerBookingService.SaveBooking(null!, new Booking()));
            Assert.That(ex!.ParamName, Is.EqualTo("model"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to save a null PhotographerBooking entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void SaveBooking_ShouldThrowArgumentNullException_WhenBookingIsNull()
        {
            // Arrange
            var photographerBooking = new PhotographerBooking
            {
                Id = Guid.NewGuid(),
                UserName = "John Doe",
                Email = "john.doe@example.com",
                Gender = "Male",
                EventDate = DateTime.Now.AddDays(1),
                StartTime = TimeSpan.FromHours(10),
                EndTime = TimeSpan.FromHours(12),
                PhotographerId = Guid.NewGuid()
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _photographerBookingService.SaveBooking(photographerBooking, null!));
            Assert.That(ex!.ParamName, Is.EqualTo("booking"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to save a null Booking entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task GetBookingListByFormDataAsync_ShouldReturnBookings_WhenValidInputsAreProvided()
        {
            // Arrange
            var model = new PhotographerBooking
            {
                Id = Guid.NewGuid(),
                UserName = "John Doe",
                Email = "john.doe@example.com",
                Gender = "Male",
                EventDate = DateTime.Now.AddDays(1),
                StartTime = TimeSpan.FromHours(10),
                EndTime = TimeSpan.FromHours(12),
                PhotographerId = Guid.NewGuid()
            };

            var bookings = new List<PhotographerBooking>
            {
                new PhotographerBooking
                {
                    Id = Guid.NewGuid(),
                    UserName = "John Doe",
                    Email = "john.doe@example.com",
                    Gender = "Male",
                    EventDate = model.EventDate,
                    StartTime = model.StartTime,
                    EndTime = model.EndTime,
                    PhotographerId = model.PhotographerId
                }
            };

            _unitOfWorkMock.Setup(u => u.PhotographerBookingRepository.GetAsync(It.IsAny<Expression<Func<PhotographerBooking, bool>>>(),  null))
    .ReturnsAsync(bookings);


            // Act
            var result = await _photographerBookingService.GetBookingListByFormDataAsync(model);

            // Assert
            Assert.That(result, Is.EqualTo(bookings));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Fetching booking list for photographer with ID: {model.PhotographerId}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task IsBooked_ShouldReturnTrue_WhenPhotographerIsBooked()
        {
            // Arrange
            var model = new PhotographerBooking
            {
                Id = Guid.NewGuid(),
                UserName = "John Doe",
                Email = "john.doe@example.com",
                Gender = "Male",
                EventDate = DateTime.Now.AddDays(1),
                StartTime = TimeSpan.FromHours(10),
                EndTime = TimeSpan.FromHours(12),
                PhotographerId = Guid.NewGuid()
            };

            var bookings = new List<PhotographerBooking>
            {
                new PhotographerBooking
                {
                    Id = Guid.NewGuid(),
                    UserName = "John Doe",
                    Email = "john.doe@example.com",
                    Gender = "Male",
                    EventDate = model.EventDate,
                    StartTime = model.StartTime,
                    EndTime = model.EndTime,
                    PhotographerId = model.PhotographerId
                }
            };

            _unitOfWorkMock.Setup(u => u.PhotographerBookingRepository.GetAsync(It.IsAny<Expression<Func<PhotographerBooking, bool>>>(), null))
    .ReturnsAsync(bookings);


            // Act
            var result = await _photographerBookingService.IsBooked(model);

            // Assert
            Assert.That(result, Is.True);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Photographer with ID: {model.PhotographerId} is booked.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }
}



