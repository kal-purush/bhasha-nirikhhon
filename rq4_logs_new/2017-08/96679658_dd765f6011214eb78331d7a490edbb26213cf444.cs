using AttachmentService.Models;
using Net4UserTokenLib;
using System;
using System.Web.Http;
using System.Web.Http.Results;

namespace AttachmentService.Controllers
{
    public class UserController : ApiController
    {
        // POST: api/UserToken
        public IHttpActionResult Post([FromBody] UserIdRequest req)
        {
            try
            {
                string userId = AttachmentService.Services.UserIdService.GetUserId(req.token);
                return Json(userId);
            }
            catch (Exception)
            {
                return new InternalServerErrorResult(this);
            }
        }
    }
}