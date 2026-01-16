using System.Collections.Generic;
using System.Linq;
using System.Net;
using Server.Data;
using Server.Exceptions;
using Server.Infrastructure;
using Server.Models;
using Server.Repository;
using Server.Services;

namespace Server.Data
{
    public class BankRepository : IBankRepository
    {
        private IRepository<Card> _cardRepository;
        private IRepository<Transaction> _transactionRepository;
        private ICardService _cardService;
        private IBusinessLogicService _businessLogicService;

        private readonly User currentUser;
        public BankRepository(IRepository<Card> cardRepository,
                              IRepository<Transaction> transactionRepository,
                              ICardService cardService,
                              IBusinessLogicService businessLogicService)
        {
            _cardRepository = cardRepository;
            _transactionRepository = transactionRepository;
            _cardService = cardService;
            _businessLogicService = businessLogicService;
        }

        public Card GetCard(string cardNumber)
        {
            var card = _cardRepository
                         .GetWithInclude(
                             c => c.CardNumber == _cardService.CreateNormalizeCardNumber(cardNumber),
                             c => c.Transactions)
                         .FirstOrDefault();

            if (card == null)
                throw new UserDataException("Card not found", cardNumber, HttpStatusCode.NotFound);

            return card;
        }

        public IEnumerable<Card> GetCards() => _cardRepository.GetAll();

        public User GetCurrentUser()
            => currentUser != null ? currentUser :
                throw new BusinessLogicException(TypeBusinessException.USER, "User is null");

        public IEnumerable<Transaction> GetTranasctions(string cardnumber, int skip, int take)
        {
            var card = GetCard(cardnumber);

            var transactions = card.Transactions.Skip(skip).Take(take);

            return transactions != null ? transactions : new List<Transaction>();
        }

        public Card OpenNewCard(string shortCardName, Currency currency, CardType cardType)
        {
            if (cardType == CardType.UNKNOWN)
                throw new UserDataException("Wrong type card", cardType.ToString());

            IList<Card> allCards = GetCards().ToList();

            var cardNumber = _businessLogicService.GenerateNewCardNumber(cardType);

            _businessLogicService.ValidateCardExist(allCards, shortCardName, cardNumber);

            var newCard = new Card
            {
                CardNumber = cardNumber,
                CardName = shortCardName,
                Currency = currency,
                CardType = cardType
            };

            _cardRepository.Add(newCard);
            _cardRepository.Save();

            var transaction = _businessLogicService.AddBonusOnOpen(newCard);

            _transactionRepository.Add(transaction);
            _transactionRepository.Save();

            return newCard;
        }

        public Transaction TransferMoney(decimal sum, string from, string to)
        {
            var cardFrom = GetCard(from);
            var cardTo = GetCard(to);

            _businessLogicService.ValidateTransfer(cardFrom, cardTo, sum);

            var fromTransaction = new Transaction
            {
                Card = cardFrom,
                CardFromNumber = cardFrom.CardNumber,
                CardToNumber = cardTo.CardNumber,
                Sum = sum
            };

            var toTransaction = new Transaction
            {
                Card = cardTo,
                DateTime = fromTransaction.DateTime,
                CardFromNumber = cardFrom.CardNumber,
                CardToNumber = cardTo.CardNumber,
                Sum = _businessLogicService.GetConvertSum(sum, cardFrom.Currency, cardTo.Currency)
            };

            _transactionRepository.Add(fromTransaction);
            _transactionRepository.Add(toTransaction);
            _transactionRepository.Save();

            return fromTransaction;
        }
    }
}