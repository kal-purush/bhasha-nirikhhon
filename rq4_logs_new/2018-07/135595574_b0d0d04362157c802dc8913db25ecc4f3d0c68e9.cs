using System;
using System.IO;
using System.Linq;
using Server.Exceptions;
using Server.Infrastructure;
using Server.Models;
using Server.Services;
using Xunit;

namespace ServerTest.ModelsTest
{
    public class CardTest
    {
        private IBusinessLogicService blservice = new BusinessLogicService();

        [Fact]
        public void AddTransactionPassed()
        {
            var user = new User("oleg@oleg.ru");
            var card = user.OpenNewCard("firstcard", Currency.RUR, CardType.VISA);

            var transaction = card.AddTransaction(new Transaction(10M, card));

            Assert.Equal(2, card.Transactions.Count);
            Assert.Equal(20M, blservice.GetBalanceOfCard(card));
        }

        [Fact]
        public void AddTransactionException()
        {
            var user = new User("oleg@oleg.ru");
            var card = new Card("4790878827491205", "4790878827491205", CardType.VISA, Currency.RUR, DateTime.Today.AddYears(-6));
            Assert.Throws<BusinessLogicException>(() => new Transaction(10M, card));
        }

        [Fact]
        public void TryDeleteTransactionException()
        {
            var user = new User("oleg@oleg.ru");
            var card = user.OpenNewCard("firstcard", Currency.RUR, CardType.VISA);
            var transaction = card.AddTransaction(new Transaction(10M, card));

            Assert.Throws<NotSupportedException>(() => card.Transactions.Remove(transaction));
        }
    }
}