namespace Ange.WebUI.Controllers
{
    using System;
    using System.Threading.Tasks;
    using Application.ChatMessage.Queries.GetChatMessageDetail;
    using Application.ChatMessage.Queries.GetChatMessageList;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;

    public class MessageController : BaseController
    {
        [HttpGet("{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<ChatMessageDetailModel>> Get(Guid id)
        {
            return Ok(await Mediator.Send(new GetChatMessageDetailQuery {Id = id}));
        }

        [HttpGet("{string}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<ChatMessageListViewModel>> GetBySubstring(string sub)
        {
            return Ok(await Mediator.Send(new GetChatMessageListQuery {SubString = sub}));
        }
    }
}