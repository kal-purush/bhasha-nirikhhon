using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Presentation
{
    [ApiController]
    [Route("api/[controller]")]
    public class BuggyController : ControllerBase
    {

        [HttpGet("notfound")] // Get : /api/Buggy/notfound
        public IActionResult GetNotFoundRequest()
        {
            // Code 
            return NotFound(); // 404
        }


        [HttpGet("servererror")] // Get : /api/Buggy/servererror
        public IActionResult GetServerErrorRequest()
        {
            throw new Exception(); 
            return Ok();
        }


        [HttpGet("badrequest")] // Get : /api/Buggy/badrequest
        public IActionResult GetBadRequest()
        {
            // Code 
            return BadRequest();  // 400
        }


        [HttpGet("badrequest/{id}")] // Get : /api/Buggy/badrequest/id
        public IActionResult GetBadRequest(int id) // Validation Error (el Mafrood a7oot Id w &atet Name masalan) 
        {
            // Code 
            return BadRequest();  // 400
        }


        [HttpGet("unauthorized")] // Get : /api/Buggy/unauthorized
        public IActionResult GetUnauthorizedRequest() // Enter place Not Allow For Him
        {
            // Code 
            return Unauthorized();  // 400
        }

    }
}