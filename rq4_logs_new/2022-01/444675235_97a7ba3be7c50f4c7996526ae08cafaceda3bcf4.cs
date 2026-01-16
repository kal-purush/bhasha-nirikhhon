
using System;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    public class ErrorTestController : BaseApiController
    {
        [HttpGet("not-found")]
        public ActionResult GetNotFound()
        {
            return NotFound();
        }

        [HttpGet("bad-request")]
        public ActionResult GetBadRequest()
        {
            return BadRequest(new ProblemDetails { Title = "Bad Request" });
        }

        [HttpGet("unauthorized")]
        public ActionResult GetUnauthorised()
        {
            return Unauthorized();
        }

        [HttpGet("validation-error")]
        public ActionResult GetValidationError()
        {
            ModelState.AddModelError("Test1", "This is GetValidationError() test 1");
            ModelState.AddModelError("Test2", "This is GetValidationError() test 2");
            return ValidationProblem();
        }

        [HttpGet("server-error")]
        public ActionResult GetServerError()
        {
            throw new Exception("This is GetServerError() test");
        }
    }
}