using AnimalShelter.Application.Services;
using AnimalShelter.Contracts.ContractsRequests;
using AnimalShelter.Contracts.ContractsResponses;
using AnimalShelter.Core.Models;
using Microsoft.AspNetCore.Mvc;

namespace AnimalShelter.WebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AdoptionApplicationsController : ControllerBase
    {
        private readonly IAdoptionApplicationsService _adoptionApplicationsService;

        public AdoptionApplicationsController(IAdoptionApplicationsService adoptionApplicationsService)
        {
            _adoptionApplicationsService = adoptionApplicationsService;
        }

        [HttpGet]
        public async Task<ActionResult<List<AdoptionApplicationsResponse>>> GetAdoptionApplications()
        {
            var adoptionApplications = await _adoptionApplicationsService.GetAllAdoptionApplications();

            var response = adoptionApplications.Select(b => new AdoptionApplicationsResponse(b.Id, b.ApplicationDate, b.Comment, b.UserId, b.AnimalId, b.StatusAdoptionId));

            return Ok(response);
        }

        [HttpPost]
        public async Task<ActionResult<Guid>> CreateAdoptionApplication([FromBody] AdoptionApplicationsRequest request)
        {
            var (adoptionApplication, error) = AdoptionApplication.Create(
                Guid.NewGuid(),
                request.ApplicationDate,
                request.Comment,
                request.UserId,
                request.AnimalId,
                request.StatusAdoptionId);

            if (!string.IsNullOrEmpty(error))
            {
                return BadRequest(error);
            }

            var adoptionApplicationId = await _adoptionApplicationsService.CreateAdoptionApplication(adoptionApplication);

            return Ok(adoptionApplicationId);
        }

        [HttpPut("{id:guid}")]
        public async Task<ActionResult<Guid>> UpdateAdoptionApplications(Guid id, [FromBody] AdoptionApplicationsRequest request)
        {
            var adoptionApplicationId = await _adoptionApplicationsService.UpdateAdoptionApplication(
                id,
                request.ApplicationDate,
                request.Comment,
                request.UserId,
                request.AnimalId,
                request.StatusAdoptionId);

            return Ok(adoptionApplicationId);
        }

        [HttpDelete("{id:guid}")]
        public async Task<ActionResult<Guid>> DeleteAdoptionApplication(Guid id)
        {
            return Ok(await _adoptionApplicationsService.DeleteAdoptionApplication(id));
        }
    }
}