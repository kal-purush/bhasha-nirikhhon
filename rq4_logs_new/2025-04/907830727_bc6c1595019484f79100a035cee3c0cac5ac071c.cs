using EasyTravel.Domain.Entites;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace UnitTest.Entities
{
    [TestFixture]
    public class HotelTests
    {
        [Test]
        public void Hotel_ShouldInitializeWithDefaultValues()
        {
            // Arrange & Act
            var hotel = new Hotel
            {
                Name = "Default Hotel",
                Address = "Default Address",
                Description = "Default Description",
                City = "Default City",
                Phone = "123-456-7890",
                Email = "default@hotel.com",
                Image = "default.jpg"
            };

            // Assert
            Assert.That(hotel.Id, Is.EqualTo(Guid.Empty));
            Assert.That(hotel.Name, Is.EqualTo("Default Hotel"));
            Assert.That(hotel.Address, Is.EqualTo("Default Address"));
            Assert.That(hotel.Description, Is.EqualTo("Default Description"));
            Assert.That(hotel.City, Is.EqualTo("Default City"));
            Assert.That(hotel.Phone, Is.EqualTo("123-456-7890"));
            Assert.That(hotel.Email, Is.EqualTo("default@hotel.com"));
            Assert.That(hotel.Rating, Is.EqualTo(3)); // Default rating
            Assert.That(hotel.Image, Is.EqualTo("default.jpg"));
            Assert.That(hotel.CreatedAt, Is.EqualTo(default(DateTime)));
            Assert.That(hotel.UpdatedAt, Is.EqualTo(default(DateTime)));
            Assert.That(hotel.Rooms, Is.Not.Null);
            Assert.That(hotel.Rooms, Is.Empty);
            Assert.That(hotel.HotelBookings, Is.Not.Null);
            Assert.That(hotel.HotelBookings, Is.Empty);
        }

        [Test]
        public void Hotel_ShouldAllowSettingProperties()
        {
            // Arrange
            var createdAt = DateTime.UtcNow;
            var updatedAt = createdAt.AddHours(1);

            var hotel = new Hotel
            {
                Name = "Grand Hotel",
                Address = "123 Main St, Anytown, USA",
                Description = "A luxurious hotel with all modern amenities.",
                City = "Anytown",
                Phone = "123-456-7890",
                Email = "info@grandhotel.com",
                Image = "hotel.jpg"
            };

            // Act
            hotel.Id = Guid.NewGuid();
            hotel.Rating = 5;
            hotel.CreatedAt = createdAt;
            hotel.UpdatedAt = updatedAt;

            // Assert
            Assert.That(hotel.Id, Is.Not.EqualTo(Guid.Empty));
            Assert.That(hotel.Name, Is.EqualTo("Grand Hotel"));
            Assert.That(hotel.Address, Is.EqualTo("123 Main St, Anytown, USA"));
            Assert.That(hotel.Description, Is.EqualTo("A luxurious hotel with all modern amenities."));
            Assert.That(hotel.City, Is.EqualTo("Anytown"));
            Assert.That(hotel.Phone, Is.EqualTo("123-456-7890"));
            Assert.That(hotel.Email, Is.EqualTo("info@grandhotel.com"));
            Assert.That(hotel.Rating, Is.EqualTo(5));
            Assert.That(hotel.Image, Is.EqualTo("hotel.jpg"));
            Assert.That(hotel.CreatedAt, Is.EqualTo(createdAt));
            Assert.That(hotel.UpdatedAt, Is.EqualTo(updatedAt));
        }

        [Test]
        public void Hotel_ShouldAllowAddingRooms()
        {
            // Arrange
            var hotel = new Hotel
            {
                Name = "Grand Hotel",
                Address = "123 Main St, Anytown, USA",
                Description = "A luxurious hotel with all modern amenities.",
                City = "Anytown",
                Phone = "123-456-7890",
                Email = "info@grandhotel.com",
                Image = "hotel.jpg"
            };

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

            // Act
            hotel.Rooms.Add(room);

            // Assert
            Assert.That(hotel.Rooms, Is.Not.Empty);
            Assert.That(hotel.Rooms.Count, Is.EqualTo(1));
            Assert.That(hotel.Rooms, Does.Contain(room));
        }

        [Test]
        public void Hotel_ShouldAllowAddingHotelBookings()
        {
            // Arrange
            var hotel = new Hotel
            {
                Name = "Grand Hotel",
                Address = "123 Main St, Anytown, USA",
                Description = "A luxurious hotel with all modern amenities.",
                City = "Anytown",
                Phone = "123-456-7890",
                Email = "info@grandhotel.com",
                Image = "hotel.jpg"
            };

            var booking = new HotelBooking
            {
                Id = Guid.NewGuid(),
                HotelId = Guid.NewGuid(),
                CheckInDate = DateTime.UtcNow,
                CheckOutDate = DateTime.UtcNow.AddDays(2),
                RoomIdsJson = "[\"101\", \"102\"]"
            };

            // Act
            hotel.HotelBookings.Add(booking);

            // Assert
            Assert.That(hotel.HotelBookings, Is.Not.Empty);
            Assert.That(hotel.HotelBookings.Count, Is.EqualTo(1));
            Assert.That(hotel.HotelBookings, Does.Contain(booking));
        }

       
    }
}
