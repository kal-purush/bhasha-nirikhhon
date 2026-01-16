using Application.Repositories;
using Application.Queries;
using Moq;
using Domain.Entities;
using System.Linq.Expressions;
using Microsoft.AspNetCore.Mvc;
using FluentAssertions;
using Application.Exceptions;
using Domain.Exceptions;

namespace UniteTest.QueryHandlerTests
{
    public class GetPaymentByReferenceTest
    {
        private readonly Mock<IPaymentRepository> _paymentRepository;
        private readonly GetPaymentByReference.Handler _handler;
        public GetPaymentByReferenceTest()
        {
            _paymentRepository = new Mock<IPaymentRepository>();
            _handler = new GetPaymentByReference.Handler(_paymentRepository.Object);
        }

        [Fact]
        public async Task Handle_PaymentExists_ReturnResponse()
        {
            //Arrange
            var Invoice = new Invoice(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid().ToString(), 2000, Domain.Enums.InvoiceStatus.Paid, "0001");

            var payment = new Payment(Invoice.Id, Guid.NewGuid().ToString(), 12000.50M, Domain.Enums.PaymentOption.Card, "0001");

            _paymentRepository.Setup(repo => repo.GetAsync(It.IsAny<Expression<Func<Payment, bool>>>())).ReturnsAsync(payment);

            var query = new GetPaymentByReference.Query(payment.Reference);

            //Act
            var response = await _handler.Handle(query, CancellationToken.None);

            //Assert
            response.Should().NotBeNull();
            response.InvoiceReference.Should().Be(payment.Invoice.Reference);
            response.PaymentOption.Should().Be(payment.Option.ToString());
            response.Amount.Should().Be(payment.Amount);
        }

        [Fact]
        public async Task Handle_PaymentDoesNotExist_ThrowsNotFoundException()
        {
            // Arrange
            var referenceNumber = Guid.NewGuid().ToString();

            _paymentRepository.Setup(repo => repo.GetAsync(It.IsAny<Expression<Func<Payment, bool>>>()))
                .ReturnsAsync((Payment?)null);

            var query = new GetPaymentByReference.Query(referenceNumber);

            // Act & Assert
            Func<Task> act = async () => await _handler.Handle(query, CancellationToken.None);

            await act.Should().ThrowAsync<NotFoundException>()
                .Where(ex => ex.Message == "Payment not found" && ex.ErrorCode.ToString() == ExceptionCodes.PaymentNotFound.ToString());
        }
    }
}