using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SpaceParkBackend.Models;
using SpaceParkBackend.Repos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SpaceParkBackend.Controllers
{
    [Route("api/v1.0/[controller]")]
    [ApiController]
    public class StarshipsController : ControllerBase
    {
        private readonly IStarshipRepository _starshipRepository;

        public StarshipsController(IStarshipRepository starshipRepository)
        {
            _starshipRepository = starshipRepository;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Starship>>> GetStarships()
        {
            var results = await _starshipRepository.GetAllStarships();
            try
            {
                if (results.Count <= 0 || results == null)
                {
                    return NotFound();
                }
                else
                {
                    return Ok();
                }
            }
            catch (Exception exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"Database failure: {exception.Message}");
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<Starship>> GetStarship(int id)
        {
            var result = await _starshipRepository.GetStarshipById(id);
            try
            {
                if (result == null)
                {
                    return NotFound();
                }
                else
                {
                    return Ok();
                }
            }
            catch (Exception exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"Database failure: {exception.Message}");
            }
        }

        [HttpPost]
        public async Task<ActionResult<Starship>> PostStarship(Starship starship)
        {
            try
            {
                await _starshipRepository.Add(starship);
                await _starshipRepository.Save();

                return CreatedAtAction("GetStarship", new { id = starship.StarshipID }, starship);
            }
            catch (Exception exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"Database Failure: {exception.Message}");
            }
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteStarship(int id)
        {
            try
            {
                var existingStarship = await _starshipRepository.GetStarshipById(id);
                if (existingStarship == null)
                {
                    return NotFound($"Could not find a starship with id {id}");
                }

                _starshipRepository.Delete(existingStarship);
                if (await _starshipRepository.Save())
                {
                    return NoContent();
                }
            }
            catch (Exception exception)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"Database Failure: {exception.Message}");
            }
            return BadRequest();
        }
    }
}