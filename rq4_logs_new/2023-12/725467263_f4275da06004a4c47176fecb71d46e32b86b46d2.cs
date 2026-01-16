using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using toiec_web.Services;
using toiec_web.Services.IService;

namespace toiec_web.Controllers
{
    [Authorize]
    public class StudentController : BaseAPIController
    {
        private readonly IStudentService _studentService;
        public StudentController(IStudentService studentService) 
        {
            _studentService = studentService;
        }
        [HttpGet]
        [Route("CheckFreeTest/{userId}")]
        public async Task<IActionResult> CheckFreeTest(string userId)
        {
            var freeTest = await _studentService.CheckFreeTest(userId);
            return Ok(freeTest);
        }
    }
}