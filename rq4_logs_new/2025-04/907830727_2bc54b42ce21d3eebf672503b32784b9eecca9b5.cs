using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace EasyTravel.Test.ApplicationTests.Services
{
    [TestFixture]
    public class PaymentOnlyServiceTests
    {
        private Mock<IApplicationUnitOfWork> _unitOfWorkMock = null!;
        private Mock<ILogger<PaymentOnlyService>> _loggerMock = null!;
        private PaymentOnlyService _paymentOnlyService = null!;

        [SetUp]
        public void SetUp()
        {
            _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
            _loggerMock = new Mock<ILogger<PaymentOnlyService>>();
            _paymentOnlyService = new PaymentOnlyService(_unitOfWorkMock.Object, _loggerMock.Object);
        }

        [Test]
        public void AddPaymentOnly_ShouldThrowArgumentNullException_WhenPaymentIsNull()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _paymentOnlyService.AddPaymentOnly(null!));
            Assert.That(ex!.ParamName, Is.EqualTo("payment"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to add a null Payment entity.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void AddPaymentOnly_ShouldAddPayment_WhenValidPaymentIsProvided()
        {
            // Arrange
            var payment = new Payment
            {
                Id = Guid.NewGuid(),
                PaymentMethod = PaymentMethods.SSLCommerz,
                Amount = 100.50m,
                PaymentDate = DateTime.Now,
                PaymentStatus = PaymentStatus.Completed,
                BookingId = Guid.NewGuid(),
                TransactionId = Guid.NewGuid()
            };

            var paymentRepositoryMock = new Mock<IPaymentRepository>();
            _unitOfWorkMock.Setup(u => u.PaymentRepository).Returns(paymentRepositoryMock.Object);

            // Act
            _paymentOnlyService.AddPaymentOnly(payment);

            // Assert
            paymentRepositoryMock.Verify(r => r.Add(payment), Times.Once);
            _unitOfWorkMock.Verify(u => u.Save(), Times.Once);

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Adding a new payment with ID: {payment.Id}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully added payment with ID: {payment.Id}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }



        [Test]
        public void AddPaymentOnly_ShouldThrowInvalidOperationException_WhenExceptionOccurs()
        {
            // Arrange
            var payment = new Payment
            {
                Id = Guid.NewGuid(),
                PaymentMethod = PaymentMethods.SSLCommerz,
                Amount = 100.50m,
                PaymentDate = DateTime.Now,
                PaymentStatus = PaymentStatus.Completed,
                BookingId = Guid.NewGuid(),
                TransactionId = Guid.NewGuid()
            };

            _unitOfWorkMock.Setup(u => u.PaymentRepository.Add(payment)).Throws(new Exception("Database error"));

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => _paymentOnlyService.AddPaymentOnly(payment));
            Assert.That(ex!.Message, Is.EqualTo($"An error occurred while adding the payment with ID: {payment.Id}."));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"An error occurred while adding the payment with ID: {payment.Id}")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void IsExist_ShouldThrowArgumentException_WhenBookingIdIsEmpty()
        {
            // Act & Assert
            var ex = Assert.ThrowsAsync<ArgumentException>(() => _paymentOnlyService.IsExist(Guid.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("bookingId"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Invalid booking ID provided for existence check.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public async Task IsExist_ShouldReturnTrue_WhenPaymentExists()
        {
            // Arrange
            var bookingId = Guid.NewGuid();
            _unitOfWorkMock.Setup(u => u.PaymentRepository.GetAsync(It.IsAny<Expression<Func<Payment, bool>>>(), null))
       .ReturnsAsync(new List<Payment> { new Payment() });

            // Act
            var result = await _paymentOnlyService.IsExist(bookingId);

            // Assert
            Assert.That(result, Is.True);

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Checking existence of payment for booking ID: {bookingId}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Payment existence check for booking ID: {bookingId} returned: True")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }      
        [Test]
        public void IsExist_ShouldThrowInvalidOperationException_WhenExceptionOccurs()
        {
            // Arrange
            var bookingId = Guid.NewGuid();
            _unitOfWorkMock.Setup(u => u.PaymentRepository.GetAsync(It.IsAny<Expression<Func<Payment, bool>>>(), null))
                .ThrowsAsync(new Exception("Database error"));

            // Act & Assert
            var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _paymentOnlyService.IsExist(bookingId));
            Assert.That(ex!.Message, Is.EqualTo($"An error occurred while checking existence of payment for booking ID: {bookingId}."));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"An error occurred while checking existence of payment for booking ID: {bookingId}")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }
}