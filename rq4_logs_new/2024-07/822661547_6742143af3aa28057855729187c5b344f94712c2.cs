using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using ShortSharing.API.Dtos.ThingDtos;
using ShortSharing.BLL.Abstractions;

namespace ShortSharing.API.Controllers.ThingsController
{
    [Route("api/[controller]")]
    [ApiController]
    public class ThingController : ControllerBase
    {
        private readonly IThingsService _thingsService;
        private readonly IMapper _mapper;

        public ThingController(IThingsService thingsService, IMapper mapper)
        {
            _thingsService = thingsService;
            _mapper = mapper;
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ThingDto>> GetByIdAsync(Guid id)
        {
            var thing = await _thingsService.GetByIdAsync(id);

            return _mapper.Map<ThingDto>(thing);            
        }

        [HttpGet("all")]
        public async Task<ActionResult<List<ThingDto>>> GetAllAsync()
        {
            var things = await _thingsService.GetAllAsync();

            return _mapper.Map<List<ThingDto>>(things);
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteAsync(Guid id)
        {
            await _thingsService.DeleteAsync(id);

            return NoContent();
        }
    }
}