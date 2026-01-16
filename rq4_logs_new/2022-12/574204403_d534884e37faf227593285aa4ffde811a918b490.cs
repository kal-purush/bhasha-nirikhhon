using Confitec.RaphaelCampos.Api.Controllers.Shared;
using Confitec.RaphaelCampos.Api.Domain.Shared;
using Confitec.RaphaelCampos.Api.Service.DTO;
using Confitec.RaphaelCampos.Api.Service.Interfaces.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confitec.RaphaelCampos.Api.Controllers
{
    /// <summary>
    /// User Controller
    /// </summary>
    public class UserController : ApiControllerBase
    {
        private readonly IUserService _userService;

        public UserController(IUserService userService)
        {
            _userService = userService;
        }

        /// <summary>
        /// Searches all registered users.
        /// </summary>
        /// <returns>Returns a list of all users</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<UserDto>), StatusCodes.Status200OK)]
        public async Task<IActionResult> GetAll()
        {
            var users = await _userService.GetAllUsersAsync();
            return Ok(users);
        }

        /// <summary>
        /// Searches for the user by ID.
        /// </summary>
        /// <param name="userId">The ID that represents the user to be searched.</param>
        /// <returns>Returns the user corresponding to the entered ID.</returns>
        [HttpGet("{userId}")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(NotFoundResult), StatusCodes.Status404NotFound)]
        public async Task<IActionResult> GetByIdAsync(int userId)
        {
            var user = await _userService.GetUserByIdAsync(userId);
            if (user is null)
            {
                return NotFound(new { Message = Constants.PROBLEM_GET_ID });
            }
            return Ok(user);
        }

        /// <summary>
        /// Searches for the user by E-mail.
        /// </summary>
        /// <param name="email">The E-mail that represents the user to be searched.</param>
        /// <returns>Returns the user corresponding to the entered ID.</returns>
        [HttpGet("email")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(NotFoundResult), StatusCodes.Status404NotFound)]
        public async Task<IActionResult> GetByEmailAsync([FromHeader] string email)
        {
            var user = await _userService.GetUserByEmailAsync(email);
            if (user is null)
            {
                return NotFound(new { Message = Constants.PROBLEM_GET_EMAIL });
            }
            return Ok(user);
        }

        /// <summary>
        /// Adds a new user record.
        /// </summary>
        /// <param name="userDto">The user entity that will be added.</param>
        /// <returns>Returns the user that was created and its information.</returns>
        [HttpPost("create")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status201Created)]
        [ProducesResponseType(typeof(BadRequestResult), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> AddUserAsync([FromBody] UserDto userDto)
        {
            var user = await _userService.AddUserAsync(userDto);
            if (user is null)
            {
                return BadRequest(new { Message = Constants.PROBLEM_CREATING });
            }
            return Created("Created", new { User = user });
        }

        /// <summary>
        /// Updates a user's record.
        /// </summary>
        /// <param name="userDto">The user entity that will be updated.</param>
        /// <returns>Returns the user that was updated and its information.</returns>
        [HttpPut("update")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(BadRequestResult), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> UpdateUserAsync([FromBody] UserDto userDto)
        {
            var user = await _userService.UpdateUserAsync(userDto);
            if (user is null)
            {
                return BadRequest(new { Message = Constants.PROBLEM_UPDATING });
            }
            return Ok(userDto);
        }

        /// <summary>
        /// Removes a user according to his ID.
        /// </summary>
        /// <param name="userId">The ID that represents the user to be removed.</param>
        /// <returns>A boolean indicating the status of the operation..</returns>
        [HttpDelete("delete/{userId}")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(BadRequestResult), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> RemoveUserByIdAsync(int userId)
        {
            var status = await _userService.RemoveUserByIdAsync(userId);
            if (!status)
            {
                return BadRequest(new { Message = Constants.PROBLEM_REMOVING });
            }
            return Ok(new { Message = Constants.SUCCESSFULLY_REMOVED });
        }

        /// <summary>
        /// Removes user from his object.
        /// </summary>
        /// <param name="userDto">The user entity that will be removed.</param>
        /// <returns>A boolean indicating the status of the operation.</returns>
        [HttpDelete("delete")]
        [ProducesResponseType(typeof(UserDto), StatusCodes.Status200OK)]
        [ProducesResponseType(typeof(BadRequestResult), StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> RemoveUserAsync([FromBody] UserDto userDto)
        {
            var status = await _userService.RemoveUserAsync(userDto);
            if (!status)
            {
                return BadRequest(new { Message = Constants.PROBLEM_REMOVING });
            }
            return Ok(new { Message = Constants.SUCCESSFULLY_REMOVED });
        }
    }
}