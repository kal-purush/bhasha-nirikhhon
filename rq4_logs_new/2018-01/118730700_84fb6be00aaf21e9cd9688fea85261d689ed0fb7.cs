using ExternalAccountWebAuthentication.Authentication;
using System.Web.Http;

namespace AndersonNotificationWeb.ApiControllers
{
    [ApiAuthorizationFilter(false, new string[0])]
    public class BaseApiController : ApiController
    {
    }
}