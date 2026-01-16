using backend.DTOs;
using backend.Services.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DisputeController : ControllerBase
    {
        private readonly IDisputeService _disputeService;

        public DisputeController(IDisputeService disputeService)
        {
            _disputeService = disputeService;
        }


        [HttpPost("add-dispute")]
        public async Task<IActionResult> CreateDispute([FromBody] AddDisputeDTOs disputeDto)
        {
            if (disputeDto == null || disputeDto.OrderId == null || string.IsNullOrWhiteSpace(disputeDto.Reason))
            {
                return BadRequest("Dispute data cannot be null ,OrderId and Reason are required.");
            }
                
            try
            {
                var createdDispute = await _disputeService.CreateDisputeAsync(disputeDto);

                return CreatedAtAction(
                    nameof(CreateDispute),
                    new { id = createdDispute.Id },
                    createdDispute
                );
            }
            catch (Exception ex)
            {
                
                return StatusCode(500, $"Internal server error: {ex.Message}");
            }
        }



        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateDispute(int id, [FromBody] AddDisputeDTOs disputeDto)
        {
            if (disputeDto == null || id <= 0)
            {
                return BadRequest("Dispute data cannot be null, Invalid dispute ID");

            }

            try
            {
                var updatedDispute = await _disputeService.UpdateDisputeAsync(id, disputeDto);

                if (updatedDispute == null)
                {
                    return NotFound($"Dispute with ID {id} not found.");
                }
                return Ok(updatedDispute);
            }
            catch (Exception ex)
            {
                
                return StatusCode(500, $"Internal server error: {ex.Message}");
            }
        }

    }
}
