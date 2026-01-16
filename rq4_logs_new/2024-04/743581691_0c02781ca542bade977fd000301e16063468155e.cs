using CourseService.API.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace CourseService.API.Feartures.CourseFearture.Queries.EvaluateCourse
{
    public class GetRatingOfUserQuerry : IRequest<IActionResult>
    {
        public int CourseId { get; set; }
        public int UserId { get; set; }

        public class GetRatingOfUserQuerryHandler : IRequestHandler<GetRatingOfUserQuerry, IActionResult>
        {
            private readonly CourseContext _context;

            public GetRatingOfUserQuerryHandler(CourseContext context)
            {
                _context = context;
            }
            public async Task<IActionResult> Handle(GetRatingOfUserQuerry request, CancellationToken cancellationToken)
            {
                var rate = _context.CourseEvaluations.FirstOrDefault(e => e.UserId == request.UserId && e.CourseId == request.CourseId);
                if (rate == null)
                {
                    return new OkObjectResult("not found");
                }
                return new OkObjectResult(rate.Star);
            }
        }
    }
}