using ClientNexus.API.Utilities;
using ClientNexus.Application.DTO;
using ClientNexus.Application.Interfaces;
using ClientNexus.Application.Services;
using ClientNexus.Domain.Entities.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ClientNexus.API.Controllers
{
    [Route("api/available-days")]
    [ApiController]
    [Authorize(Policy = "IsServiceProvider")]
    public class AvailableDayController : ControllerBase
    {
        private readonly IAvailableDayService _availableDayService;

        public AvailableDayController(IAvailableDayService availableDayService)
        {
            _availableDayService = availableDayService;
        }
        [HttpGet]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public async Task<ActionResult<IEnumerable<AvailableDayDTO>>> GetAvailableDays()
        {
            var userId = User.GetId();
            if (userId is null)
                return Unauthorized();
            var availableDays = await _availableDayService.GetByServiceProviderAsync(userId.Value);
            return Ok(availableDays);
        }

        [HttpGet("{id:int}", Name = "GetAvailableDayById")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]

        public async Task<ActionResult<AvailableDayDTO>> GetAvailableDayById(int id)
        {
            var userId = User.GetId();
            var availableDay = await _availableDayService.GetByIdAsync(id);
            
            // Check if the available day belongs to the current user
            if (availableDay.ServiceProviderId != userId)
                return Unauthorized("You do not have permission to view this availability pattern");

            return Ok(availableDay); 
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public async Task<ActionResult<AvailableDayDTO>> CreateAvailableDay([FromBody] AvailableDayCreateDTO createDTO)
        {
            var userId = User.GetId();
            if (userId is null)
                return Unauthorized();
            var availableDay = await _availableDayService.CreateAsync(createDTO, userId.Value);
            return CreatedAtRoute("GetAvailableDayById", new { id = availableDay.Id }, availableDay);

        }
        [HttpPut("{availableDayId:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public async Task<IActionResult> UpdateAvailableDay(int availableDayId, [FromBody] AvailableDayUpdateDTO updateDTO)
        {
            var userId = User.GetId();
            if (userId is null)
                return Unauthorized();

            await _availableDayService.UpdateAsync(availableDayId,userId.Value, updateDTO);
            return NoContent();
        }
        [HttpDelete("{availableDayId:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]
        public async Task<IActionResult> DeleteAvailableDay(int availableDayId)
        {
            var userId = User.GetId();
            if (userId is null)
                return Unauthorized();

            await _availableDayService.DeleteAsync(availableDayId, userId.Value);
            return NoContent();

        }

    }
}