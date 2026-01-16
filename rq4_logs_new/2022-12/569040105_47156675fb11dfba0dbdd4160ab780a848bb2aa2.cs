using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dotnet_course_management.Models;
using Microsoft.AspNetCore.Mvc;

namespace dotnet_course_management.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BasicCourseController : ControllerBase
    {
        [HttpGet]
        public async Task<ActionResult<List<Course>>> GetCourses()
        {
            return new List<Course> { new Course {
                Id = 1,
                Name = "Math 101",
                Description = "The fundamentals of counting",
                Units = 3
            },
            new Course {
                Id = 2,
                Name = "Math 102",
                Description = "The fundamentals of addition",
                Units = 3
            }
            };
        }
    }
}