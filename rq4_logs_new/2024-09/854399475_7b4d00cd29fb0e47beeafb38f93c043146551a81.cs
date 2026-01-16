using Microsoft.AspNetCore.Mvc;
using System.Reflection;

namespace API2.Controllers
{
    public class ExceptionController : BaseAPIController
    {
        [HttpGet("not-found")]
        public IActionResult GetNotFound()
        {
            return NotFound();
        }

        [HttpGet("bad-request")]
        public IActionResult GetBadRequest()
        {
            return BadRequest(new ProblemDetails{Title= "This is a bad request" });
        }
        [HttpGet("un-authorized")]
        public IActionResult GetUnauthorized()
        {
            return Unauthorized();
        }
        [HttpGet("validation-error")]
        public IActionResult GetValidationError()
        {
            ModelState.AddModelError("Prob1", "Error 1");
            ModelState.AddModelError("Prob2", "Error 2");
            return ValidationProblem();
        }
        [HttpGet("server-error")]
        public IActionResult GetServerError()
        {
            throw new Exception("This is a server error");
        }
    }
}