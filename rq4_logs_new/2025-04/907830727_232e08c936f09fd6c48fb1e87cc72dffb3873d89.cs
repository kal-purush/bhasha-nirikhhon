using EasyTravel.Domain.Entites;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace UnitTest.Entities
{
    [TestFixture]
    public class BookingTests
    {
        [Test]
        public void Booking_ShouldInitializeWithDefaultValues()
        {
            // Arrange & Act
            var booking = new Booking
            {
                Id = Guid.NewGuid(),
                BookingStatus = BookingStatus.Pending,
                BookingTypes = BookingTypes.Hotel,
                TotalAmount = 200.50m,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
                Payments = new List<Payment>()
            };

            // Assert
            Assert.That(booking.Id, Is.Not.EqualTo(Guid.Empty));
            Assert.That(booking.BookingStatus, Is.EqualTo(BookingStatus.Pending));
            Assert.That(booking.BookingTypes, Is.EqualTo(BookingTypes.Hotel));
            Assert.That(booking.TotalAmount, Is.EqualTo(200.50m));
            Assert.That(booking.CreatedAt, Is.Not.EqualTo(default(DateTime)));
            Assert.That(booking.UpdatedAt, Is.Not.EqualTo(default(DateTime)));
            Assert.That(booking.Payments, Is.Empty);
        }
    }
}