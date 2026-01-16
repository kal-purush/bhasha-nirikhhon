using iPhoneBE.Data.ViewModels.ChatDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class ChatController : ControllerBase
    {
        private readonly IChatServices _chatService;

        public ChatController(IChatServices chatService)
        {
            _chatService = chatService;
        }

        [HttpGet("rooms")]
        public async Task<ActionResult<List<ChatRoomViewModel>>> GetUserChatRooms()
        {
            var userId = User.Identity?.Name;
            if (string.IsNullOrEmpty(userId))
                return Unauthorized();

            var rooms = await _chatService.GetUserChatRooms(userId);
            return Ok(rooms);
        }

        [HttpGet("room/{roomId}")]
        public async Task<ActionResult<ChatRoomViewModel>> GetChatRoom(int roomId, [FromQuery] int pageSize = 20, [FromQuery] int pageNumber = 1)
        {
            try
            {
                var room = await _chatService.GetChatRoom(roomId, pageSize, pageNumber);
                return Ok(room);
            }
            catch (KeyNotFoundException ex)
            {
                return NotFound(ex.Message);
            }
        }

        [HttpPost("room/private")]
        public async Task<ActionResult<ChatRoomViewModel>> CreatePrivateRoom([FromBody] string otherUserId)
        {
            var userId = User.Identity?.Name;
            if (string.IsNullOrEmpty(userId))
                return Unauthorized();

            try
            {
                var room = await _chatService.CreatePrivateRoom(userId, otherUserId);
                return Ok(room);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [HttpGet("users/online")]
        public async Task<ActionResult<List<ChatParticipantViewModel>>> GetOnlineUsers()
        {
            var users = await _chatService.GetOnlineUsers();
            return Ok(users);
        }
    }
}