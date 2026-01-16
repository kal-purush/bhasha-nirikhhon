using AireBugTrackerApi.Helpers;
using DatabaseContext.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Services.Interfaces;
using Services.Models;

namespace AireBugTrackerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        private readonly ILogger<UserController> _logger;
        private IUserService _userService;

        public UserController(ILogger<UserController> logger, IUserService userService)
        {
            _logger = logger;
            _userService = userService;
        }

        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var response = await _userService.GetAllAsync();
            _logger.LogDebug(LogEvents.GetItems, response.Message);

            return Ok(response.Target);
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> Get(int id)
        {
            try
            {
                var response = await _userService.GetByIdAsync(id);
                _logger.LogDebug(LogEvents.GetItem, response.Message);

                return Ok(response.Target);
            }
            catch (Exception e)
            {
                _logger.LogError(LogEvents.GetItemInternalError, e, null);

                return StatusCode(500);
            }
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] UserDTO user)
        {
            try
            {
                var response = await _userService.CreateAsync(user);
                _logger.LogDebug(LogEvents.CreateItem, response.Message);

                return StatusCode((int)response.Status, response.Target);
            }
            catch (Exception e)
            {
                _logger.LogError(LogEvents.CreateItemInternalError, e, null);

                return StatusCode(500);
            }
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> Put(int id, [FromBody] UserDTO user)
        {
            try
            {
                var response = await _userService.UpdateAsync(id, user);
                _logger.LogDebug(LogEvents.UpdateItem, response.Message);

                return StatusCode((int)response.Status, response.Target);
            }
            catch (Exception e)
            {
                _logger.LogError(LogEvents.UpdateItemInternalError, e, null);

                return StatusCode(500);
            }
        }
    }
}