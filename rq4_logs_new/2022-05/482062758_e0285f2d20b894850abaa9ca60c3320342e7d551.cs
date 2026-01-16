using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CapstoneManager.Repositories;
using CapstoneManager.Models;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;

namespace CapstoneManager.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class ClassController : ControllerBase
    {
        private readonly ITeacherRepository _teacherRepo;
        private readonly IClassRepository _classRepository;
        public ClassController(ITeacherRepository teacherRepo, IClassRepository classRepo)
        {
            _teacherRepo = teacherRepo;
            _classRepository = classRepo;
        }
        private Teacher GetCurrentTeacher()
        {
            var firebaseUserId = User.FindFirst(ClaimTypes.NameIdentifier).Value;
            return _teacherRepo.GetByFirebaseUserId(firebaseUserId);
        }
        [HttpGet]
        public IActionResult GetTeachersClasses()
        {
            var teacher = GetCurrentTeacher();
            return Ok(_classRepository.GetClassesByTeacherId(teacher.Id));
        }
    }
}