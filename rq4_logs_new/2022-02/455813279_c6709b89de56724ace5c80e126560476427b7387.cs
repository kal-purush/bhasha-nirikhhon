using Microsoft.AspNetCore.Mvc;


namespace OLT.Extensions.SwaggerGen.Tests.Controllers.V2
{
    [ApiVersion("2.0")]
    [ApiVersion("3.0")]
    [Route("api/another-test")]
    [ApiController]
    public class AnotherTestController : ControllerBase
    {

        /// <summary>
        /// Log Method
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [HttpPost, Route("")]
        [ApiVersion("2.0", Deprecated = true)]
        public ActionResult<string> Log(string value)
        {
            return Ok($"Received {value}");
        }

        [HttpGet, Route("Pong")]
        [ApiVersion("2.0", Deprecated = true)]
        public ActionResult<string> Ping(int? value = null)
        {
            return Ok("Ping");
        }

        [HttpGet, Route("{value1:int}/test/{value2}")]
        public ActionResult<string> Test(int value1, int? value2 = null)
        {
            return Ok($"Got this -> {value1} {value2}");
        }

        /// <summary>
        /// Creates an Employee.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST api/another/1234/test?api-version=1.0
        ///     {        
        ///       "firstName": "Mike",
        ///       "lastName": "Andrew",
        ///       "emailId": "Mike.Andrew@gmail.com"        
        ///     }
        /// </remarks>
        /// <param name="value1"></param>   
        /// <param name="model"></param>   
        [HttpGet, Route("bogus/{value1:int}/test")]
        public ActionResult<string> PostTest(int value1, [FromBody] TestModel model)
        {
            return Ok($"Got this -> {value1} {model.Id} {model.Value}");
        }
    }
}