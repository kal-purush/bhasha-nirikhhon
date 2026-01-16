using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CapstoneManager.Repositories;
using Microsoft.AspNetCore.Authorization;

namespace CapstoneManager.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class StudentController : ControllerBase
    {
        private readonly IStudentRepository _studentRepo;
        public StudentController(IStudentRepository studentRepo)
        {
            _studentRepo = studentRepo;
        }
        [HttpGet("{id}")]
        public IActionResult GetStudentsByClassId(int id)
        {
            return Ok(_studentRepo.GetStudentsByClassId(id));
        }
    }
}