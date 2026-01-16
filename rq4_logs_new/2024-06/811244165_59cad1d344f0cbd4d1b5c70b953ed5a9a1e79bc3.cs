using AutoMapper;
using HealthMate.API.DTO.Models;
using HealthMate.API.DTO.ShortModels;
using HealthMate.BLL.Abstractions;
using HealthMate.BLL.Models;
using Microsoft.AspNetCore.Mvc;

namespace HealthMate.API.Controllers
{
    [Route("api/mood")]
    [ApiController]

    public class MoodController(IMoodService moodService, IMapper mapper) : ControllerBase
    {
        [HttpGet(Name = "MoodByDate")]
        public async Task<IActionResult> GetMoodByDate([FromQuery] DateTime date, CancellationToken token)
        {
            var mood = await moodService.GetMoodByDate(date, token);

            var moodDto = mapper.Map<MoodDTO>(mood);

            return Ok(moodDto);
        }

        [HttpGet]
        public async Task<IActionResult> GetMoodsBetweenTwoDates([FromQuery] DateTime startDate,
            DateTime finishDate,
            CancellationToken token)
        {
            var moods = await moodService.GetMoodsBetweenTwoDates(startDate, finishDate, token);

            var moodsDto = mapper.Map<ICollection<MoodDTO>>(moods);

            return Ok(moodsDto);
        }

        [HttpPost]
        public async Task<IActionResult> CreateMood([FromQuery] DateTime date,
            ShortMoodDTO moodModelForCreation,
            CancellationToken token)
        {
            var moodModel = mapper.Map<Mood>(moodModelForCreation);

            var newMood = await moodService.CreateAsync(moodModel, token);

            return CreatedAtRoute("MoodByDate", new { date = newMood.Date }, newMood);
        }

        [HttpDelete("{id:guid}")]
        public async Task<IActionResult> DeleteMood(Guid id, CancellationToken token)
        {
            await moodService.DeleteAsync(id, token);

            return NoContent();
        }

        [HttpPut]
        public async Task<IActionResult> UpdateMood(Guid id,
            ShortMoodDTO modelForUpdate,
            CancellationToken token)
        {
            var moodModel = mapper.Map<Mood>(modelForUpdate);

            await moodService.UpdateAsync(id, moodModel, token);

            return NoContent();
        }
    }
}