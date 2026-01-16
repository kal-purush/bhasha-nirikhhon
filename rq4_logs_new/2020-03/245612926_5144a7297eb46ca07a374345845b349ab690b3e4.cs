using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CourseGenerator.BLL.DTO;
using CourseGenerator.BLL.Interfaces;
using CourseGenerator.DAL.Pagination;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CourseGenerator.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CoursesController : ControllerBase
    {
        private readonly ICourseService _courseService;
        
        public CoursesController(ICourseService courseService)
        {
            _courseService = courseService;
        }

        [HttpGet]
        public async Task<IActionResult> GetForUserByPhoneNumberPaged(
            string phoneNumber, string langCode, int pageSize, int pageIndex = 1)
        {
            PagedList<CourseItemDTO> courseItemsPaged = await _courseService.GetByPhoneWithLangPagedAsync(
                phoneNumber, langCode, pageSize, pageIndex);

            if (courseItemsPaged == null)
                return BadRequest("There is no user with this phone number");
            if (courseItemsPaged.Items.Count() == 0)
                return NoContent();

            return Ok(courseItemsPaged);
        }
    }
}