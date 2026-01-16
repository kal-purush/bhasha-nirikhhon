
using System.Linq;
using System.Net;
using System.Net.Http;

using System.Web.Http.Controllers;
using System.Web.Http.Filters;

using System.Security.Claims;
using System.Threading;
using Quiz.BLL.ModelServices;

namespace Quiz.API.ActionFilters
{
    public class AuthorizationRequiredAttribute: ActionFilterAttribute
    {
        private const string Token = "Token";
        private bool _isPermissionRequired=true;

        /// <summary>
        /// Content Permission Validation
        /// </summary>
        /// <param name="isPermissionRequired">By default required</param>
        public AuthorizationRequiredAttribute(bool isPermissionRequired)
        {
            _isPermissionRequired = isPermissionRequired;
        }

        public AuthorizationRequiredAttribute()
        {
            _isPermissionRequired = true;
        }

        public override void OnActionExecuting(HttpActionContext filterContext)
        {
            //  Get API key provider
            var tokenService = filterContext.ControllerContext.Configuration
            .DependencyResolver.GetService(typeof(ITokenService)) as ITokenService;

            if (filterContext.Request.Headers.Contains(Token))
            {
                var token = filterContext.Request.Headers.GetValues(Token).First();

                // Validate Token
                var isValidToken = tokenService.ValidateToken(token);

                if (!isValidToken)
                {
                    var responseMessage = new HttpResponseMessage(HttpStatusCode.Unauthorized) { ReasonPhrase = "Unauthorized" };
                    filterContext.Response = responseMessage;
                }
                //else if (_isPermissionRequired /* && !tokenService.IsUserInRole(token, "", "")*/)
                //{
                //    var responseMessage = new HttpResponseMessage(HttpStatusCode.BadRequest) { ReasonPhrase = "Permission Required" };
                //    filterContext.Response = responseMessage;
                //}

            }
            else
            {
                filterContext.Response = new HttpResponseMessage(HttpStatusCode.Unauthorized);
            }

            base.OnActionExecuting(filterContext);

        }
    }
}