using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Server.Data;
using Server.Exceptions;
using Server.Models;
using Server.Services;

namespace Server.Controllers
{
    [Route("api/[controller]")]
    public class CardsController : Controller
    {
        private readonly IBankRepository _repository;

        private readonly ICardService _cardService;

        public CardsController(IBankRepository repository, ICardService cardService)
        {
            _repository = repository;
            _cardService = cardService;
        }

        // GET api/cards
        [HttpGet]
        public IEnumerable<Card> Get() => _repository.GetCards();

        // GET api/cards/5334343434343...
        [HttpGet("{number}")]
        public Card Get(string number)
        {
            if (!_cardService.CheckCardEmmiter(number))
                throw new UserDataException("Card number is invalid", number);
            //TODO validation
            return _repository.GetCard(number);
        }

        // POST api/cards
        [HttpPost]
        public IActionResult Post([FromBody]string cardType)
            => throw new NotImplementedException();

        // DELETE api/cards/5
        [HttpDelete("{number}")]
        public IActionResult Delete(string number) => StatusCode(405);

        //TODO PUT method
    }
}