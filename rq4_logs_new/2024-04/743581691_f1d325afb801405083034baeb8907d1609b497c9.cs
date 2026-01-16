using CourseService.API.Feartures.CourseFearture.Command.EvaluateCourse;
using CourseService.API.Feartures.CourseFearture.Queries.EvaluateCourse;
using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CourseService.API.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class EvaluateCourseController : ControllerBase
    {
        private readonly IMediator _mediator;

        public EvaluateCourseController(IMediator mediator)
        {
            _mediator = mediator;
        }

       
        [HttpPost]
        public async Task<ActionResult<int>> AddCourseEvaluation([FromBody] CreateEvaluateCourseCommand request)
        {
            var id = await _mediator.Send(request);
            return Ok(id);
        }

        
        [HttpPut]
        public async Task<ActionResult> UpdateCourseEvaluation(int id, [FromBody] UpdateCourseEvaluationRequest request)
        {
            if (id != request.Id)
                return BadRequest();

            await _mediator.Send(request);
            return NoContent();
        }
        [HttpGet]
        public async Task<ActionResult<double>> GetCourseAverageRating(int courseId)
        {
            var request = new CalculateCourseAverageRatingQuerry { CourseId = courseId };
            var averageRating = await _mediator.Send(request);
            return Ok(averageRating);
        }

    }
}