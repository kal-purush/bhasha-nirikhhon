using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Server.Data;
using Server.Exceptions;
using Server.Infrastructure;
using Server.Models;
using Server.Services;

namespace Server.Controllers
{
    [Route("api/[controller]")]
    public class TransactionsController : Controller
    {
        private readonly IBankRepository _repository;

        private readonly ICardService _cardService;

        private readonly IBusinessLogicService _businessLogicServer;

        public TransactionsController(IBankRepository repository, ICardService cardService, IBusinessLogicService businessLogicServer)
        {
            _repository = repository;
            _cardService = cardService;
            _businessLogicServer = businessLogicServer;
        }

        // GET api/transactions/5334343434343?skip=...
        [HttpGet("{number}")]
        public IEnumerable<TransactionDto> Get(string number, [FromQuery] int skip)
        {
            if (!_cardService.CheckCardEmmiter(number))
                throw new UserDataException("Card number is invalid", number);

            if (skip < 0)
                throw new UserDataException("Skip must be greater than -1", skip.ToString());

            var transactions = _repository.GetTranasctions(number, skip, 10);

            return transactions.Select(transaction => new TransactionDto
            {
                DateTime = transaction.DateTime,
                From = transaction.CardFromNumber,
                To = transaction.CardToNumber,
                Sum = transaction.Sum,
                Credit = transaction.CardToNumber == _cardService.CreateNormalizeCardNumber(number)
            });
        }

        // POST api/transactions
        [HttpPost]
        public IActionResult Post([FromBody] TransactionDto value)
        {
            if (value == null) throw new UserDataException("transaction data is null", null);

            _businessLogicServer.ValidateTransferDto(value);

            var transaction = _repository.TransferMoney(value.Sum, value.From, value.To);

            return Created($"/transactions/{_cardService.CreateNormalizeCardNumber(value.From)}", new TransactionDto
            {
                DateTime = transaction.DateTime,
                From = transaction.CardFromNumber,
                To = transaction.CardToNumber,
                Sum = transaction.Sum,
                Credit = transaction.CardToNumber == _cardService.CreateNormalizeCardNumber(value.From)
            });
        }

        // DELETE api/transactions
        [HttpDelete]
        public IActionResult Delete() => StatusCode(405);

        // PUT api/transactions
        [HttpPut]
        public IActionResult Put() => StatusCode(405);
    }
}