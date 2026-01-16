using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using planit_data.DTOs;
using planit_data.Entities;
using planit_data.Services;

namespace planit_api.Controllers
{
    public class CardController : ApiController
    {

        CardService cs = new CardService();

        // GET: api/Card
        public IEnumerable<ReadCardDTO> Get()
        {
            return cs.GetAllCards();
        }

        // GET: api/Card/5
        public ReadCardDTO Get(int id)
        {
            return cs.GetCardById(id);
        }

        // POST: api/Card
        public void Post([FromBody]CreateCardDTO value)
        {
            if (value != null)
                cs.InsertCard(value);
        }

        // PUT: api/Card/5
        public void Put([FromBody]UpdateCardDTO value)
        {
            if (value != null)
                cs.UpdateCard(value);
        }

        [Route("api/Card/{cardId:int}/Move/{listId:int}")]
        [HttpPut]
        public void Move(int cardId, int listId)
        {
            cs.MoveCardToList(cardId, listId);
        }

        // DELETE: api/Card/5
        public void Delete(int id)
        {
            cs.DeleteCard(id);
        }
    }
}