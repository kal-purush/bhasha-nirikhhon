using Application.Dtos;
using Application.Interfaces.RepoInterface;
using Application.Jobbapplication.JobbApplicationCommands.CreateJobbApplication;
using Domain.Models;
using Infrastructure.Repository;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class JobbApplicationController : ControllerBase
    {
        private readonly IRepository<JobbApplication> _jobbApplicationRepository;

        public JobbApplicationController(IRepository<JobbApplication> jobbApplicationRepository)
        {
            _jobbApplicationRepository = jobbApplicationRepository;
        }

        [HttpGet]
        public async Task<IActionResult> GetAllJobbAplications()
        {
            var jobbApplications = await _jobbApplicationRepository.GetAllAsync();
            return Ok(jobbApplications);
        }

        [HttpGet("{id:guid}")]
        public async Task<IActionResult> GetJobbaplicationsById(Guid id, CancellationToken cancellationToken)
        {
            var jobbApplication = await _jobbApplicationRepository.GetByIdAsync(id, cancellationToken);
            if (jobbApplication == null)
            {
                return NotFound("Job application not found.");
            }

            return Ok(jobbApplication);
        }

        [HttpPost]
        public async Task<IActionResult> CreateJobbApplication([FromBody] CreateJobbApplicationCommand jobbApplication)
        {
            var createdJobbApplication = await _jobbApplicationRepository.CreateAsync(new JobbApplication
            {
                JobTitle = jobbApplication.JobTitle,
                CompanyName = jobbApplication.CompanyName,
                ApplicationDate = DateTime.UtcNow,
                Status = false,
                UserId = Guid.NewGuid() // Assuming UserId is generated here, adjust as necessary
            });

            return CreatedAtAction(nameof(GetJobbaplicationsById), new { id = createdJobbApplication.Id }, createdJobbApplication);
        }

        [HttpPut("{id:guid}")]
        public async Task<IActionResult> UpdateJobbApplication(Guid id, [FromBody] JobbApplication jobbApplication, CancellationToken cancellationToken)
        {
            if (id != jobbApplication.Id)
            {
                return BadRequest("Job application ID mismatch.");
            }

            await _jobbApplicationRepository.UpdateAsync(jobbApplication, cancellationToken);
            return NoContent();
        }

        [HttpDelete("{id:guid}")]
        public async Task<IActionResult> Delete(Guid id)
        {
            var result = await _jobbApplicationRepository.DeleteByIdAsync(id);
            if (result == "Entity not found")
            {
                return NotFound(result);
            }

            return Ok(result);
        }
    }
}