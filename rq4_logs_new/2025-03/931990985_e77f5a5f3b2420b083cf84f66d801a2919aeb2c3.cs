using MealService.Application.DTOs.Cuisines;
using MealService.Application.UseCases.Cuisines.Commands.AddCuisine;
using MealService.Application.UseCases.Cuisines.Commands.DeleteCuisine;
using MealService.Application.UseCases.Cuisines.Commands.UpdateCuisine;
using MealService.Application.UseCases.Cuisines.Queries.GetCuisines;
using MealService.Application.UseCases.Cuisines.Queries.GetCuisinesByName;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace MealService.API.Controllers
{
    [Route("api/cuisines")]
    [ApiController]
    public class CuisineController : ControllerBase
    {
        private readonly ILogger<CuisineController> _logger;
        private readonly IMediator _mediator;

        public CuisineController(IMediator mediator, ILogger<CuisineController> logger)
        {
            _mediator = mediator;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> GetCuisines(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start retrieving cuisines.");

            var cuisines = await _mediator.Send(new GetCuisinesQuery(), cancellationToken);

            _logger.LogInformation("Cuisines retrieved.");

            return Ok(cuisines);
        }

        [HttpGet("{name}")]
        public async Task<IActionResult> GetCuisinesByName(string name, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start retrieving cuisine by its name.");

            var cuisines = await _mediator.Send(new GetCuisinesByNameQuery(name), cancellationToken);

            _logger.LogInformation("Cuisines retrieved.");

            return Ok(cuisines);
        }

        [Authorize(Policy = "Admin")]
        [HttpPost("cuisine/create")]
        public async Task<IActionResult> CreateCuisine([FromForm] CuisineRequestDto cuisine, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start adding new cuisine.");

            var newCuisine = await _mediator.Send(new AddCuisineCommand(cuisine), cancellationToken);
            _logger.LogInformation("New cuisine added.");

            return CreatedAtAction(nameof(CreateCuisine), new { id = newCuisine.Id }, newCuisine);
        }

        [Authorize(Policy = "Admin")]
        [HttpPut("cuisine/update/{id}")]
        public async Task<IActionResult> UpdateCuisine(Guid id, [FromForm] CuisineRequestDto cuisine, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start updating cuisine {id}.");

            var updatedCuisine = await _mediator.Send(new UpdateCuisineCommand(id, cuisine), cancellationToken);

            _logger.LogInformation($"Cuisine {id} updated.");

            return Ok(updatedCuisine);
        }

        [Authorize(Policy = "Admin")]
        [HttpDelete("cuisine/delete/{cuisineId}")]
        public async Task<IActionResult> DeleteCuisine(Guid cuisineId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start deleting cuisine {cuisineId}.");

            await _mediator.Send(new DeleteCuisineCommand(cuisineId), cancellationToken);

            _logger.LogInformation($"cuisine {cuisineId} deleted.");

            return StatusCode(204);
        }
    }
}