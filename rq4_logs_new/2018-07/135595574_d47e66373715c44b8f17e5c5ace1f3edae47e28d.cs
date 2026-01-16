using Server.Controllers;
using Server.Data;
using Xunit;
using Moq;
using System.Linq;
using System.Collections.Generic;
using Server.Models;
using Server.Services;
using Server.ViewModels;
using Server.Infrastructure;
using Microsoft.AspNetCore.Mvc;
using Server.Exceptions;

namespace ServerTest.ControllersTest
{
    public class TransactionsControllerTest
    {
        private readonly ICardService _cardService = new CardService();
        private readonly IBusinessLogicService _businessLogicService = new BusinessLogicService(new CardService());

        private readonly FakeDataGenerator fakeDataGenerator;
        public TransactionsControllerTest()
        {
            fakeDataGenerator = new FakeDataGenerator(_businessLogicService);
        }

        [Theory]
        [InlineData("1234 1234 1233 1234")]
        [InlineData("12341233123")]
        [InlineData("")]
        [InlineData(null)]
        [InlineData("5395029009021990")]
        [InlineData("4978588211036789")]
        public void GetTransactionsException(string value)
        {
            var mock = new Mock<IBankRepository>();
            var controller = new TransactionsController(mock.Object, _cardService, _businessLogicService);

            Assert.Throws<UserDataException>(() => controller.Get(value, 0));
        }

        [Theory]
        [InlineData("4083969259636239")]
        [InlineData("5101265622568232")]
        public void GetTransactionsPassed(string value)
        {
            // Arrange
            var mock = new Mock<IBankRepository>();
            //var mockCards = new FakeDataGenerator(_businessLogicService).GenerateFakeCards();
            var mockCard = fakeDataGenerator.GenerateFakeCard(value);
            var mockTran = fakeDataGenerator.GenerateFakeTransactions(mockCard);

            mock.Setup(r => r.GetTranasctions(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>())).Returns(mockTran);

            var controller = new TransactionsController(mock.Object, _cardService, _businessLogicService);

            // Test
            var transactions = controller.Get(value, 0);

            // Assert
            mock.Verify(r => r.GetTranasctions(value, 0, 10), Times.AtMostOnce());
            Assert.Equal(4, transactions.Count());
        }
    }
}