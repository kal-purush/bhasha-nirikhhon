using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TripPlannerBackend.API.Dto;
using TripPlannerBackend.DAL;
using TripPlannerBackend.DAL.Entity;

namespace TripPlannerBackend.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccommodationController : ControllerBase
    {
        private readonly TripPlannerDbContext _context;
        private readonly IMapper _mapper;
        public AccommodationController(TripPlannerDbContext context, IMapper mapper)
        {
            _context = context;
            _mapper = mapper;
        }

        [HttpPost]
        public async Task<ActionResult<GetAccommodationDto>> Create([FromBody] CreateAccommodationDto createAccommodationDto)
        {
            System.Diagnostics.Debug.WriteLine(createAccommodationDto);
            Accommodation accommodation = _mapper.Map<Accommodation>(createAccommodationDto);
            await _context.Accommodations.AddAsync(accommodation);
            await _context.SaveChangesAsync();
            GetAccommodationDto getAccommodationDto = _mapper.Map<GetAccommodationDto>(accommodation);

            return CreatedAtAction(nameof(Create), new { id = getAccommodationDto.Id }, getAccommodationDto);
        }

        [HttpGet("{destinationId}")]
        public async Task<ActionResult<List<GetAccommodationDto>>> GetByDestinationId(int destinationId)
        {
            List<Accommodation> accommodations = await _context.Accommodations.Where(a => a.DestinationId == destinationId).ToListAsync();

            if (accommodations == null)
            {
                return NotFound();
            }

            return _mapper.Map<List<GetAccommodationDto>>(accommodations);
        }

        [HttpPut("{id}")]
        public async Task<ActionResult<GetAccommodationDto>> Update(int id, [FromBody] CreateAccommodationDto updateAccommodationDto)
        {
            Accommodation? accommodation = await _context.Accommodations.FindAsync(id);

            if (accommodation == null) return NotFound();

            Accommodation updatedAccommodation = _mapper.Map(updateAccommodationDto, accommodation);
            await _context.SaveChangesAsync();
            GetAccommodationDto getAccommodationDto = _mapper.Map<GetAccommodationDto>(updatedAccommodation);

            return CreatedAtAction(nameof(Update), new { id = id }, getAccommodationDto);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> Delete(int id)
        {
            Accommodation? accommodation = await _context.Accommodations.FindAsync(id);
            if (accommodation == null) return NotFound();

            _context.Accommodations.Remove(accommodation);
            await _context.SaveChangesAsync();

            return NoContent();
        }
    }
}