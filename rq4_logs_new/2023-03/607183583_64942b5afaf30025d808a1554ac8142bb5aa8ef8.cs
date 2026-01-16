using dbcontext;
using dbcontext.Classes;
using Microsoft.AspNetCore.Mvc;

namespace Linktindr_be.Controllers
{
    public class BaseController : ControllerBase
    {
        protected readonly OurContext OC;

        public BaseController(OurContext OC)
        {
            this.OC = OC;
        }

        protected bool IsAuthenticated()
        {
            return HttpContext.Items.ContainsKey("User");
        }

        protected User GetUser()
        {
            bool hasAuthorization = HttpContext.Items.ContainsKey("User");
            if (hasAuthorization)
            {
                return (User)HttpContext.Items["User"];
            }

            return null;
        }

        protected bool IsMedewerker()
        {
            User? u = GetUser();

            return u != null && u.GetUserType() == UserType.MEDEWERKER;
        }

        protected bool IsTalentmanager()
        {
            User? u = GetUser();

            return u != null && u.GetUserType() == UserType.TALENTMANAGER;
        }

        protected bool IsOpdrachtgever()
        {
            User? u = GetUser();

            return u != null && u.GetUserType() == UserType.OPDRACHTGEVER;
        }

    }
}