using AutoMapper;
using GradeBook.BusinessLogic.Interfaces;
using GradeBook.DataAccess.Entities;
using GradeBook.Models.Read;
using GradeBook.Models.Write;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GradeBook.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize(Roles = "Admin")]
    public class AdminController : ControllerBase
    {
        private readonly IAdminService _adminService;
        private readonly IMapper _mapper;

        public AdminController(IAdminService adminService, IMapper mapper)
        {
            _adminService = adminService;
            _mapper = mapper;
        }

        [HttpGet("users")]
        public IEnumerable<UserModel> GetUsers() => _adminService.GetUsers();

        [HttpPost("pupil")]
        public Task CreatePupil([FromBody] CreatePupil createPupil) => _adminService.Create<Pupil>(_mapper.Map<Pupil>(createPupil));

        [HttpPost("teacher")]
        public Task CreateTeacher([FromBody] CreateTeacher createTeacher) => 
            _adminService.Create<Teacher>(_mapper.Map<Teacher>(createTeacher));
    }
}