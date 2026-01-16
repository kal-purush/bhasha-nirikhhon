using AnimalShelter.Application.Services;
using AnimalShelter.Contracts;
using AnimalShelter.Core.Models;
using Microsoft.AspNetCore.Mvc;
using AnimalShelter.Contracts.ContractsRequests;
using AnimalShelter.DataAccess;
using Microsoft.EntityFrameworkCore;

namespace AnimalShelter.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AnimalsController : ControllerBase
    {
        private readonly IAnimalsService _animalsService;

        public AnimalsController(IAnimalsService animalsService)
        {
            _animalsService = animalsService;
        }

        [HttpGet]
        public async Task<ActionResult<List<AnimalsResponse>>> GetAnimals()
        {
            var animals = await _animalsService.GetAllAnimals();

            var response = animals.Select(b => new AnimalsResponse(b.Id, b.Name, b.Gender, b.Age, b.Description, b.Photo, b.TypeAnimalId, b.AnimalStatusId));

            return Ok(response);
        }

        [HttpPost]
        public async Task<ActionResult<Guid>> CreateAnimal([FromBody] AnimalsRequest request)
        {
            var (animal, error) = Animal.Create(
                Guid.NewGuid(),
                request.Name,
                request.Gender,
                request.Age,
                request.Description,
                request.Photo,
                request.TypeAnimalId,
                request.AnimalStatusId);

            if (!string.IsNullOrEmpty(error))
            {
                return BadRequest(error);
            }

            var animalId = await _animalsService.CreateAnimal(animal);

            return Ok(animalId);
        }

        [HttpPut("{id:guid}")]
        public async Task<ActionResult<Guid>> UpdateAnimals(Guid id, [FromBody] AnimalsRequest request)
        {
            var animalId = await _animalsService.UpdateAnimal(
                id, 
                request.Name,
                request.Gender,
                request.Age,
                request.Description,
                request.Photo,
                request.TypeAnimalId,
                request.AnimalStatusId);

            return Ok(animalId);
        }

        [HttpDelete("{id:guid}")]
        public async Task<ActionResult<Guid>> DeleteAnimal(Guid id)
        {
            return Ok(await _animalsService.DeleteAnimal(id));
        }
    }
}