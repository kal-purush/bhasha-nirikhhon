using InternalSystem.UserManagement.API.Helpers;
using IntrenalSystem.UserManagement.Model.APIRequestModels;
using IntrenalSystem.UserManagement.Model.APIResponseModels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace InternalSystem.UserManagement.API.Controllers.V1
{
    [Route("api/authentication")]
    [ApiController]
    public class AuthenticationController : ControllerBase
    {
        #region Constructor
        public AuthenticationController()
        {

        }
        #endregion

        #region ActionMethods
        [HttpPost("authenticate")]
        public async Task<IActionResult> Authenticate(AuthenticationRequestBody authenticationRequestBody)
        {
            var response = new JsonResponseModel<AuthenticationRequestBody>();
            if (!ModelState.IsValid)
            {
                response.Message = ModelState.GetErrors();
                response.HasError = true;
                response.StatusCode = HttpStatusCode.BadRequest;
                return this.GetResponse(response);
            }

            //Code to validate username/password 
            var user = ValidateUserCredentials(
               authenticationRequestBody.UserName,
               authenticationRequestBody.Password);

        }


        //#endregion
    }
}