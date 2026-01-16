using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace EasyTravel.Test.ApplicationTests.Services
{
    [TestFixture]
    public class RoomServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock = null!;
        private Mock<ILogger<RoomService>> _loggerMock = null!;
        private RoomService _roomService = null!;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<RoomService>>();
            _roomService = new RoomService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void Create_ShouldThrowArgumentNullException_WhenRoomIsNull()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _roomService.Create(null!));
            Assert.That(ex!.ParamName, Is.EqualTo("entity"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to create a null Room entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Create_ShouldAddRoomAndSave_WhenRoomIsValid()
        {
            // Arrange
            var room = new Room
            {
                Id = Guid.NewGuid(),
                RoomNumber = "101",
                RoomType = "Deluxe",
                PricePerNight = 150.00m,
                MaxOccupancy = 2,
                Description = "A deluxe room with a king-size bed.",
                IsAvailable = true
            };

            var roomRepositoryMock = new Mock<IRoomRepository>();
            _unitOfWorkMock.Setup(u => u.RoomRepository).Returns(roomRepositoryMock.Object);

            // Act
            _roomService.Create(room);

            // Assert
            roomRepositoryMock.Verify(r => r.Add(room), Times.Once);
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully created room with ID: {room.Id}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }



        [Test]
        public void Delete_ShouldThrowArgumentException_WhenIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _roomService.Delete(Guid.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("id"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Invalid room ID provided for deletion.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Delete_ShouldThrowKeyNotFoundException_WhenRoomDoesNotExist()
        {
            // Arrange
            var roomId = Guid.NewGuid();
            _unitOfWorkMock.Setup(u => u.RoomRepository.GetById(roomId)).Returns((Room?)null);

            // Act
            var ex = Assert.Throws<InvalidOperationException>(() => _roomService.Delete(roomId));

            // Assert
            Assert.Multiple(() =>
            {
                Assert.That(ex, Is.Not.Null, "An exception should be thrown.");
                Assert.That(ex!.Message, Is.EqualTo($"An error occurred while deleting the room with ID: {roomId}."));
                Assert.That(ex.InnerException, Is.TypeOf<KeyNotFoundException>(), "The inner exception should be a KeyNotFoundException.");
                Assert.That(ex.InnerException?.Message, Is.EqualTo($"Room with ID: {roomId} not found."));
            });

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Room with ID: {roomId} not found for deletion.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }



        [Test]
        public void Delete_ShouldRemoveRoomAndSave_WhenRoomExists()
        {
            // Arrange
            var roomId = Guid.NewGuid();
            var room = new Room
            {
                Id = roomId,
                RoomNumber = "101",
                RoomType = "Deluxe",
                PricePerNight = 150.00m,
                MaxOccupancy = 2,
                Description = "A deluxe room with a king-size bed.",
                IsAvailable = true
            };
            _unitOfWorkMock.Setup(u => u.RoomRepository.GetById(roomId)).Returns(room);

            // Act
            _roomService.Delete(roomId);

            // Assert
            _unitOfWorkMock.Verify(u => u.RoomRepository.Remove(room), Times.Once);
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully deleted room with ID: {roomId}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Get_ShouldThrowArgumentException_WhenIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _roomService.Get(Guid.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("id"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Invalid room ID provided for retrieval.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Get_ShouldReturnRoom_WhenRoomExists()
        {
            // Arrange
            var roomId = Guid.NewGuid();
            var room = new Room
            {
                Id = roomId,
                RoomNumber = "101",
                RoomType = "Deluxe",
                PricePerNight = 150.00m,
                MaxOccupancy = 2,
                Description = "A deluxe room with a king-size bed.",
                IsAvailable = true
            };
            _unitOfWorkMock.Setup(u => u.RoomRepository.GetById(roomId)).Returns(room);

            // Act
            var result = _roomService.Get(roomId);

            // Assert
            Assert.That(result, Is.EqualTo(room));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Fetching room with ID: {roomId}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void GetAll_ShouldReturnAllRooms()
        {
            // Arrange
            var rooms = new List<Room>
            {
                new Room
                {
                    Id = Guid.NewGuid(),
                    RoomNumber = "101",
                    RoomType = "Deluxe",
                    PricePerNight = 150.00m,
                    MaxOccupancy = 2,
                    Description = "A deluxe room with a king-size bed.",
                    IsAvailable = true
                },
                new Room
                {
                    Id = Guid.NewGuid(),
                    RoomNumber = "102",
                    RoomType = "Standard",
                    PricePerNight = 100.00m,
                    MaxOccupancy = 2,
                    Description = "A standard room with a queen-size bed.",
                    IsAvailable = true
                }
            };
            _unitOfWorkMock.Setup(u => u.RoomRepository.GetAll()).Returns(rooms);

            // Act
            var result = _roomService.GetAll();

            // Assert
            Assert.That(result, Is.EqualTo(rooms));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Fetching all rooms.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Update_ShouldThrowArgumentNullException_WhenRoomIsNull()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _roomService.Update(null!));
            Assert.That(ex!.ParamName, Is.EqualTo("entity"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to update a null Room entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Update_ShouldEditRoomAndSave_WhenRoomIsValid()
        {
            // Arrange
            var room = new Room
            {
                Id = Guid.NewGuid(),
                RoomNumber = "101",
                RoomType = "Deluxe",
                PricePerNight = 150.00m,
                MaxOccupancy = 2,
                Description = "A deluxe room with a king-size bed.",
                IsAvailable = true
            };

            var roomRepositoryMock = new Mock<IRoomRepository>();
            _unitOfWorkMock.Setup(u => u.RoomRepository).Returns(roomRepositoryMock.Object);

            // Act
            _roomService.Update(room);

            // Assert
            roomRepositoryMock.Verify(r => r.Edit(room), Times.Once);
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully updated room with ID: {room.Id}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }




        [Test]
        public void GetRoomByHotel_ShouldThrowArgumentException_WhenHotelIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _roomService.GetRoomByHotel(Guid.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("id"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Invalid hotel ID provided for fetching rooms.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void GetRoomByHotel_ShouldReturnRooms_WhenHotelIdIsValid()
        {
            // Arrange
            var hotelId = Guid.NewGuid();
            var rooms = new List<Room>
            {
                new Room
                {
                    Id = Guid.NewGuid(),
                    RoomNumber = "101",
                    RoomType = "Deluxe",
                    PricePerNight = 150.00m,
                    MaxOccupancy = 2,
                    Description = "A deluxe room with a king-size bed.",
                    IsAvailable = true,
                    HotelId = hotelId
                },
                new Room
                {
                    Id = Guid.NewGuid(),
                    RoomNumber = "102",
                    RoomType = "Standard",
                    PricePerNight = 100.00m,
                    MaxOccupancy = 2,
                    Description = "A standard room with a queen-size bed.",
                    IsAvailable = true,
                    HotelId = hotelId
                }
            };
            _unitOfWorkMock.Setup(u => u.RoomRepository.GetRooms(hotelId)).Returns(rooms);

            // Act
            var result = _roomService.GetRoomByHotel(hotelId);

            // Assert
            Assert.That(result, Is.EqualTo(rooms));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully fetched rooms for hotel with ID: {hotelId}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }
}
