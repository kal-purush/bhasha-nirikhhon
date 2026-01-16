using System;
using System.Diagnostics;
using System.Web.Http;
using LessComplexWebApi.Errors;

namespace LessComplexWebApi.Controllers
{
    [RoutePrefix("api/v1/goose")]
    public class GooseController : ApiController
    {
        // GET api/v1/goose/find/myname
        [HttpGet]
        [Route("find/{name}")]
        [NotFoundFilter]
        public IHttpActionResult FindGoose(string name)
        {
            FindGooldenGoose();
            return Ok();
        }

        public void FindGooldenGoose()
        {
            throw new NullReferenceException("This goose is not you are looking for.");
        }
    }
}