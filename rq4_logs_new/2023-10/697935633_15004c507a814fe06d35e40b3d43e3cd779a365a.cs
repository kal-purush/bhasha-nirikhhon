using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using vabalas_api.Repositories;

namespace vabalas_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class JobController : ControllerBase
    {
        private readonly IJobRepository _jobRepository;
        public JobController(IJobRepository jobRepository)
        {
            _jobRepository = jobRepository;
        }
        [HttpGet]
        public async Task<IActionResult> GetAllJobs()
        {
            var job = await _jobRepository.GetAll();
            return Ok(job);
        }
    }
}