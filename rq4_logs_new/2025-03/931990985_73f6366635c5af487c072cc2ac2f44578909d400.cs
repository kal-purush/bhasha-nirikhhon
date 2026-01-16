using MealService.Application.DTOs.Tags;
using MealService.Application.UseCases.Tags.Commands.AddTag;
using MealService.Application.UseCases.Tags.Commands.DeleteTag;
using MealService.Application.UseCases.Tags.Commands.UpdateTag;
using MealService.Application.UseCases.Tags.Queries.GetTags;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace MealService.API.Controllers
{
    [Route("api/tags")]
    [ApiController]
    public class TagController : ControllerBase
    {
        private readonly ILogger<TagController> _logger;
        private readonly IMediator _mediator;

        public TagController(IMediator mediator, ILogger<TagController> logger)
        {
            _mediator = mediator;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> GetTags(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start retrieving tags.");

            var tags = await _mediator.Send(new GetTagsQuery(), cancellationToken);

            _logger.LogInformation("Tags retrieved.");

            return Ok(tags);
        }

        [Authorize(Policy = "Admin")]
        [HttpPost("tag/create")]
        public async Task<IActionResult> CreateTag([FromForm] TagRequestDto tag, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start adding new tag.");

            var newTag = await _mediator.Send(new AddTagCommand(tag), cancellationToken);
            _logger.LogInformation("New tag added.");

            return CreatedAtAction(nameof(CreateTag), new { id = newTag.Id }, newTag);
        }

        [Authorize(Policy = "Admin")]
        [HttpPut("tag/update/{id}")]
        public async Task<IActionResult> UpdateTag(Guid id, [FromForm] TagRequestDto tag, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start updating tag {id}.");

            var updatedTag = await _mediator.Send(new UpdateTagCommand(id, tag), cancellationToken);

            _logger.LogInformation($"Tag {id} updated.");

            return Ok(updatedTag);
        }

        [Authorize(Policy = "Admin")]
        [HttpDelete("tag/delete/{tagId}")]
        public async Task<IActionResult> DeleteMeal(Guid tagId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start deleting tag {tagId}.");

            await _mediator.Send(new DeleteTagCommand(tagId), cancellationToken);
            _logger.LogInformation($"Tag {tagId} deleted.");

            return StatusCode(204);
        }
    }
}