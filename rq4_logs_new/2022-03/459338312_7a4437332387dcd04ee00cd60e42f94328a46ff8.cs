using Chat_App_DAL.Interfaces;
using Chat_App_DAL.Models;
using Microsoft.AspNetCore.Mvc;

namespace chat_app.Controllers
{
    [Route("api/channel")]
    [ApiController]
    public class ChannelsController : ControllerBase
    {
        private readonly IChannelRepository _channelRepository;
        public ChannelsController(IChannelRepository channelRepository)
        {
            _channelRepository = channelRepository;
        }

        [HttpPost("new-channel")]
        public async Task<ActionResult<Channel>> CreateNewChannel([FromBody] Channel channel)
        {
            bool isValidChannel = await _channelRepository.ValidateChannel(channel);

            if (!isValidChannel)
            {
                return BadRequest("Channel already exists");
            }

            Channel newChannel = await _channelRepository.CreateChannel(channel);
            return Ok(newChannel);
        }


        // get messages from every user in a chatroom
        [HttpGet("messages")]
        public void GetChatroomMessages(string token)
        {

        }

    }
}