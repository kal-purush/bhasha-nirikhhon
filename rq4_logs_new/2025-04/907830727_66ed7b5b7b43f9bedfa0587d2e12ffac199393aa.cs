using EasyTravel.Domain.Entites;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace UnitTest.Entities
{
    [TestFixture]
    public class BusBookingTests
    {
        [Test]
        public void BusBooking_ShouldInitializeWithDefaultValues()
        {
            // Arrange & Act
            var booking = new BusBooking
            {
                Id = Guid.NewGuid(),
                BusId = Guid.NewGuid(),
                PassengerName = "John Smith",
                Email = "john.smith@example.com",
                PhoneNumber = "123-456-7890",
                BookingDate = DateTime.UtcNow,
                SelectedSeats = new List<string> { "A1", "A2" },
                SelectedSeatIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() }
            };

            // Assert
            Assert.That(booking.Id, Is.Not.EqualTo(Guid.Empty));
            Assert.That(booking.BusId, Is.Not.EqualTo(Guid.Empty));
            Assert.That(booking.PassengerName, Is.EqualTo("John Smith"));
            Assert.That(booking.Email, Is.EqualTo("john.smith@example.com"));
            Assert.That(booking.PhoneNumber, Is.EqualTo("123-456-7890"));
            Assert.That(booking.BookingDate, Is.Not.EqualTo(default(DateTime)));
            Assert.That(booking.SelectedSeats, Has.Count.EqualTo(2));
            Assert.That(booking.SelectedSeatIds, Has.Count.EqualTo(2));
        }
    }
}