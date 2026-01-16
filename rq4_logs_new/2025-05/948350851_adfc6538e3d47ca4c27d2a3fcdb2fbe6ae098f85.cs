using ForkAndSpoon.Application.Ingredients.Commands.CreateIngredient;
using ForkAndSpoon.Application.Ingredients.Commands.DeleteIngredient;
using ForkAndSpoon.Application.Ingredients.Commands.UpdateIngredient;
using ForkAndSpoon.Application.Ingredients.Queries.GetAllIngredients;
using ForkAndSpoon.Application.Ingredients.Queries.GetIngredientById;
using ForkAndSpoon.Application.Ingredients.Queries.GetIngredientByName;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace ForkAndSpoon.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class IngredientController : ControllerBase
    {
        private readonly IMediator _mediator;

        public IngredientController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpGet]
        public async Task<IActionResult> GetAllIngredients()
        {
            var result = await _mediator.Send(new GetAllIngredientsQuery());

            if (!result.IsSuccess)
                return BadRequest(result.ErrorMessage);

            return Ok(result);
        }

        [HttpGet("{ingredientId}")]
        public async Task<IActionResult> GetIngredientById(int ingredientId)
        {
            var result = await _mediator.Send(new GetIngredientByIdQuery(ingredientId));

            if (!result.IsSuccess)
                return NotFound(result.ErrorMessage);

            return Ok(result);
        }

        [HttpGet("by-name/{name}")]
        public async Task<IActionResult> GetIngredientByName(string name)
        {
            var result = await _mediator.Send(new GetIngredientByNameQuery(name));

            if (!result.IsSuccess)
                return NotFound(result.ErrorMessage);

            return Ok(result);
        }

        [Authorize(Roles = "Admin")]
        [HttpPost]
        public async Task<IActionResult> CreateIngredient([FromBody] CreateIngredientCommand command)
        {
            var result = await _mediator.Send(command);

            if (!result.IsSuccess || result.Data == null)
                return BadRequest(result.ErrorMessage);

            return CreatedAtAction(nameof(GetIngredientById), new { ingredientId = result.Data.IngredientID }, result);
        }

        [Authorize(Roles = "Admin")]
        [HttpPut("{ingredientId}")]
        public async Task<IActionResult> UpdateIngredient(int ingredientId, [FromBody] string updatedName)
        {
            var result = await _mediator.Send(new UpdateIngredientCommand(ingredientId, updatedName));

            if (!result.IsSuccess)
                return NotFound(result.ErrorMessage);

            return Ok(result);
        }

        [Authorize(Roles = "Admin")]
        [HttpDelete("{ingredientId}")]
        public async Task<IActionResult> DeleteIngredient(int ingredientId)
        {
            var result = await _mediator.Send(new DeleteIngredientCommand(ingredientId));

            if (!result.IsSuccess)
                return NotFound(result.ErrorMessage);

            return NoContent();
        }
    }
}