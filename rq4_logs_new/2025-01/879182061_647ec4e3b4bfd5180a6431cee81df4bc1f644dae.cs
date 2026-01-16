using Microsoft.AspNetCore.Mvc;
using OnlineLearningWebAPI.DTOs.request.CourseTag;
using OnlineLearningWebAPI.Service.IService;

namespace OnlineLearningWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CourseTagController : ControllerBase
    {
        private readonly ICourseTagService _courseTagService;

        public CourseTagController(ICourseTagService courseTagService)
        {
            _courseTagService = courseTagService;
        }

        [HttpGet("course/{courseId}")]
        public async Task<IActionResult> GetTagsByCourseId(int courseId)
        {
            var tags = await _courseTagService.GetTagsByCourseIdAsync(courseId);
            return Ok(tags);
        }

        [HttpPost]
        public async Task<IActionResult> CreateTag([FromBody] CreateCourseTagDTO createCourseTagDTO)
        {
            var result = await _courseTagService.CreateTagAsync(createCourseTagDTO);
            if (!result) return BadRequest(new { message = "Failed to create tag" });

            return Ok(new { message = "Tag created successfully" });
        }

        [HttpDelete("{courseTagId}")]
        public async Task<IActionResult> DeleteTag(int courseTagId)
        {
            var result = await _courseTagService.DeleteTagAsync(courseTagId);
            if (!result) return NotFound(new { message = "Tag not found or delete failed" });

            return Ok(new { message = "Tag deleted successfully" });
        }
    }
}