using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Tokengram.Models.DTOS.HTTP.Requests;
using Tokengram.Models.DTOS.HTTP.Responses;
using Tokengram.Models.DTOS.Shared.Responses;
using Tokengram.Services.Interfaces;

namespace Tokengram.Controllers
{
    [Authorize]
    [ApiController]
    [Route("[controller]")]
    public class UserController : BaseController
    {
        private readonly IMapper _mapper;
        private readonly IUserService _userService;
        private readonly IPostService _postService;
        private readonly IChatService _chatService;

        public UserController(
            IMapper mapper,
            IUserService userService,
            IPostService postService,
            IChatService chatService
        )
        {
            _mapper = mapper;
            _userService = userService;
            _postService = postService;
            _chatService = chatService;
        }

        [HttpPut]
        public async Task<ActionResult<UserResponseDTO>> UpdateUser([FromForm] UserUpdateRequest request)
        {
            var result = await _userService.UpdateUser(GetUserAddress(), request);

            return Ok(_mapper.Map<UserResponseDTO>(result));
        }

        [HttpGet("posts")]
        public async Task<ActionResult<IEnumerable<UserPostResponseDTO>>> GetUserPosts(
            [FromQuery] PaginationRequestDTO request
        )
        {
            var result = await _postService.GetUserPosts(request, GetUserAddress());

            return Ok(_mapper.Map<IEnumerable<UserPostResponseDTO>>(result));
        }

        [HttpGet("chat-profile")]
        public async Task<ActionResult<UserChatProfileResponseDTO>> GetUserChatProfile()
        {
            var result = await _chatService.GetUserChatProfile(GetUserAddress());

            return Ok(_mapper.Map<UserChatProfileResponseDTO>(result));
        }
    }
}