using API.Data;
using API.Entites;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Data.Common;

namespace API.Controllers
{
    
    public class BuggyController : BaseApiController
    {
        private readonly DataContext _dataContext;

        public BuggyController(DataContext dataContext )
        {
            _dataContext = dataContext;
        }

        [HttpGet("auth")]
        public ActionResult<string> GetSecret()
        {
            return "secret text";
        }

        [HttpGet("not-found")]
        public ActionResult<AppUser> GetNotFound()
        {
            var things = _dataContext.User.Find(-1);

            if(things== null)
            {
                return NotFound();
            }

            return things;
        }

        [HttpGet("server-error")]
        public ActionResult<string> GetServerError()
        {
            //var thing = _dataContext.User.Find(-1);

            //var thingsToReturn = thing.ToString();

            //return thingsToReturn;
            var thing = _dataContext.User.Find(-1);

            if (thing == null)
            {
                throw new Exception("The requested resource was not found.");
            }

            var thingsToReturn = thing.ToString();

            return thingsToReturn;
        }

        [HttpGet("bad-request")]
        public ActionResult<string> GetBadRequest()
        {


            return BadRequest("This was not a good request");
            //throw new Exception("This is a bad request error!");
        }
    }

    //public class BadRequestException : Exception
    //{
    //    public BadRequestException(string message) : base(message) { }
    //}
}